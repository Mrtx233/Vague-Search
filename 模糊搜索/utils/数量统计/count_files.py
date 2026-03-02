import os
from typing import Tuple, Dict

# 全局字典：存储样张文件子文件夹的汇总文件数 {子文件夹名: 总文件数}
sample_folder_summary: Dict[str, int] = {}

def count_files_in_dir(dir_path: str) -> Tuple[int, list]:
    """统计单个文件夹直接文件数，返回(文件数, 子文件夹路径列表)"""
    file_count = 0
    sub_dirs = []
    try:
        for item in os.listdir(dir_path):
            item_abs_path = os.path.join(dir_path, item)
            if os.path.isfile(item_abs_path):
                file_count += 1
            elif os.path.isdir(item_abs_path):
                sub_dirs.append(item_abs_path)
    except PermissionError:
        print(f"⚠️  权限不足，无法访问：{dir_path}")
    except Exception as e:
        print(f"❌ 访问文件夹失败 {dir_path}：{str(e)}")
    return file_count, sub_dirs

def traverse_dir_hierarchy(root_path: str, level: int = 0, is_last: bool = False, prefix: str = "") -> None:
    """
    递归遍历文件夹（生成标准树形）+ 收集样张文件子文件夹数据
    :param root_path: 当前遍历文件夹路径
    :param level: 当前层级（根目录为0）
    :param is_last: 是否为上级文件夹的最后一个子文件夹
    :param prefix: 层级缩进前缀（传递树形结构的竖线/空格）
    """
    global sample_folder_summary
    file_num, sub_dirs = count_files_in_dir(root_path)
    dir_name = os.path.basename(root_path)
    sub_dir_count = len(sub_dirs)

    # 打印当前文件夹树形信息
    branch = "└─ " if is_last else "├─ "
    print(f"{prefix}{branch}{dir_name}---->{file_num}")

    # 核心：识别「样张文件」文件夹，统计其子文件夹并汇总
    if dir_name == "样张文件":
        for sub_dir in sub_dirs:
            sub_name = os.path.basename(sub_dir)
            # 获取当前样张子文件夹的文件数
            sub_file_num, _ = count_files_in_dir(sub_dir)
            # 汇总：存在则累加，不存在则初始化
            if sub_name in sample_folder_summary:
                sample_folder_summary[sub_name] += sub_file_num
            else:
                sample_folder_summary[sub_name] = sub_file_num

    # 生成子文件夹的缩进前缀，保证树形对齐
    child_prefix = prefix + ("│  " if not is_last else "   ")

    # 递归遍历子文件夹
    for idx, sub_dir in enumerate(sub_dirs):
        sub_is_last = (idx == sub_dir_count - 1)
        traverse_dir_hierarchy(sub_dir, level + 1, sub_is_last, child_prefix)

def print_sample_folder_summary():
    """打印样张文件子文件夹的汇总统计结果"""
    if not sample_folder_summary:
        print("\n⚠️  未找到「样张文件」文件夹，无需汇总统计")
        return
    # 按文件数降序排序（便于查看数据分布）
    sorted_summary = sorted(sample_folder_summary.items(), key=lambda x: x[1], reverse=True)
    # 打印汇总标题和结果
    print("\n" + "="*60)
    print("📊 所有「样张文件」文件夹子目录 跨目录汇总统计（按文件数降序）")
    print("="*60)
    for sub_name, total_num in sorted_summary:
        print(f"{sub_name:<15} → 总文件数：{total_num:,}")  # 千分位分隔，提升可读性
    print("="*60)

if __name__ == "__main__":
    # 目标绝对路径（按需修改）
    TARGET_ROOT_PATH = r"D:\数据采集\0227\常规2\ET组件"

    # 根路径合法性校验
    if not os.path.exists(TARGET_ROOT_PATH):
        print(f"错误：指定的路径不存在 → {TARGET_ROOT_PATH}")
    elif not os.path.isdir(TARGET_ROOT_PATH):
        print(f"错误：指定的路径不是文件夹 → {TARGET_ROOT_PATH}")
    else:
        print(f"根路径：{TARGET_ROOT_PATH}\n")
        # 遍历文件夹，生成树形并收集样张数据
        traverse_dir_hierarchy(TARGET_ROOT_PATH, is_last=True)
        # 打印样张文件子文件夹汇总统计
        print_sample_folder_summary()