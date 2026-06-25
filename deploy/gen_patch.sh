#!/usr/bin/env bash
#
# gen_patch.sh - 生成当前分支与目标分支之间的差异补丁目录
#
# 用法:
#   ./gen_patch.sh [当前分支] [目标分支]
#
# 参数:
#   当前分支  可选, 默认从 git 当前分支读取
#   目标分支  可选, 默认 2.3.12-hw-prod
#
# 输出:
#   在 deploy 目录下创建 patch_当前分支_目标分支 目录,
#   内部按照原仓库目录结构保存差异文件.
#

set -euo pipefail

# -------------------- 路径定位 --------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# -------------------- 参数解析 --------------------
CURRENT_BRANCH="${1:-}"
TARGET_BRANCH="${2:-2.3.12-hw-prod}"

cd "${PROJECT_ROOT}"

# 当前分支默认从 git 读取
if [[ -z "${CURRENT_BRANCH}" ]]; then
    if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
        echo "[ERROR] 当前目录不是 git 仓库: ${PROJECT_ROOT}" >&2
        exit 1
    fi
    CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
    if [[ -z "${CURRENT_BRANCH}" || "${CURRENT_BRANCH}" == "HEAD" ]]; then
        echo "[ERROR] 无法确定当前分支 (detached HEAD?)，请显式传入当前分支名" >&2
        exit 1
    fi
fi

# -------------------- 校验目标分支 --------------------
if ! git show-ref --verify --quiet "refs/heads/${TARGET_BRANCH}" \
   && ! git show-ref --verify --quiet "refs/remotes/origin/${TARGET_BRANCH}"; then
    echo "[ERROR] 目标分支不存在: ${TARGET_BRANCH}" >&2
    echo "        (已检查 refs/heads 和 refs/remotes/origin)" >&2
    exit 1
fi

# -------------------- 准备输出目录 --------------------
PATCH_DIR="${SCRIPT_DIR}/patch_${CURRENT_BRANCH}_${TARGET_BRANCH}"
echo "[INFO] 当前分支: ${CURRENT_BRANCH}"
echo "[INFO] 目标分支: ${TARGET_BRANCH}"
echo "[INFO] 输出目录: ${PATCH_DIR}"

rm -rf "${PATCH_DIR}"
mkdir -p "${PATCH_DIR}"

# -------------------- 更新目标分支 (git pull) --------------------
# 策略:
#   - 若当前 == 目标, 直接在当前分支 pull
#   - 若当前 != 目标, 暂存工作区 → 切到目标 → pull → 切回 → 恢复 stash
#   - pull 失败不中断脚本, 仅打印警告 (使用本地版本继续比较)
STASH_REF=""

restore_workspace() {
    if [[ -n "${STASH_REF}" ]]; then
        echo "[INFO] 恢复暂存的工作区 (${STASH_REF})..."
        git stash pop "${STASH_REF}" >/dev/null 2>&1 \
            || echo "[WARN] 恢复暂存失败, 请手动执行: git stash pop ${STASH_REF}" >&2
        STASH_REF=""
    fi
}
trap restore_workspace EXIT

if [[ "${CURRENT_BRANCH}" == "${TARGET_BRANCH}" ]]; then
    echo "[INFO] 已在目标分支, 执行 git pull..."
    if ! git pull --ff-only; then
        echo "[WARN] git pull 失败, 将使用本地版本继续比较" >&2
    fi
else
    echo "[INFO] 切换到目标分支 ${TARGET_BRANCH} 执行 git pull..."

    # 检查是否有未提交/未跟踪的内容, 有则暂存
    if ! git diff --quiet HEAD 2>/dev/null \
       || [[ -n "$(git ls-files --others --exclude-standard)" ]]; then
        echo "[INFO] 检测到未提交内容, 暂存..."
        STASH_REF="$(git stash push -u -m "gen_patch.sh auto-stash")" || {
            echo "[ERROR] git stash 失败" >&2
            exit 1
        }
    fi

    if ! git checkout "${TARGET_BRANCH}"; then
        echo "[ERROR] 无法切换到 ${TARGET_BRANCH}" >&2
        exit 1
    fi

    if ! git pull --ff-only; then
        echo "[WARN] git pull 失败, 将使用本地版本继续比较" >&2
    fi

    if ! git checkout "${CURRENT_BRANCH}"; then
        echo "[ERROR] 无法切回 ${CURRENT_BRANCH}, 请手动处理" >&2
        exit 1
    fi

    # 回到当前分支后清空 trap, 由 restore_workspace 正常处理
    restore_workspace
    trap - EXIT
fi

# -------------------- 收集差异文件 --------------------
# 范围: 当前分支独有的提交, 即 ${TARGET_BRANCH}..${CURRENT_BRANCH}
DIFF_FILES="$(git diff --name-only "${TARGET_BRANCH}...${CURRENT_BRANCH}" || true)"

if [[ -z "${DIFF_FILES}" ]]; then
    echo "[WARN] ${TARGET_BRANCH} 与 ${CURRENT_BRANCH} 之间没有差异文件"
    exit 0
fi

echo "[INFO] 发现差异文件:"
echo "${DIFF_FILES}" | sed 's/^/        /'

# -------------------- 复制文件 (保持目录结构) --------------------
COPIED=0
SKIPPED=0
while IFS= read -r FILE; do
    [[ -z "${FILE}" ]] && continue

    SRC="${PROJECT_ROOT}/${FILE}"
    DST="${PATCH_DIR}/${FILE}"

    if [[ ! -e "${SRC}" ]]; then
        # 文件在目标分支存在, 在当前分支被删除 —— 跳过
        echo "[SKIP] 已删除: ${FILE}"
        SKIPPED=$((SKIPPED + 1))
        continue
    fi

    mkdir -p "$(dirname "${DST}")"
    if [[ -d "${SRC}" ]]; then
        # 罕见: 差异里出现目录项 (如 submodule)
        echo "[SKIP] 是目录: ${FILE}"
        SKIPPED=$((SKIPPED + 1))
        continue
    fi

    cp -p "${SRC}" "${DST}"
    COPIED=$((COPIED + 1))
done <<< "${DIFF_FILES}"

echo "------------------------------------------------"
echo "[DONE] 已复制 ${COPIED} 个文件, 跳过 ${SKIPPED} 个"
echo "[DONE] 补丁目录: ${PATCH_DIR}"
