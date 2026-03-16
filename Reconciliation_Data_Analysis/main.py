#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据处理标准化流程 - 完整版
功能：整合Operations数据（01.csv, 02.csv, 03.csv）和QA数据，生成聚合报表
输出：result.csv（按日期+设备ID聚合）
"""

import pandas as pd
import warnings
warnings.filterwarnings('ignore')


# ==================== 工具函数 ====================

def clean_date(date_str):
    """统一日期格式为 YYYY-MM-DD"""
    if pd.isna(date_str) or date_str == '':
        return None
    
    date_str = str(date_str).strip()
    
    try:
        if '年' in date_str and '月' in date_str and '日' in date_str:
            date_str = date_str.replace('年', '-').replace('月', '-').replace('日', '')
            dt = pd.to_datetime(date_str, format='%Y-%m-%d', errors='coerce')
        else:
            dt = pd.to_datetime(date_str, errors='coerce')
        
        return dt.strftime('%Y-%m-%d') if pd.notna(dt) else None
    except:
        return None


def clean_device_id(device_id):
    """统一设备ID格式 - 提取后缀部分"""
    if pd.isna(device_id) or device_id == '':
        return None
    
    device_id = str(device_id).strip().lower()
    
    # 如果包含"-"，提取最后一部分（如 "071-b852" -> "b852"）
    if '-' in device_id:
        device_id = device_id.split('-')[-1]
    
    return device_id if device_id else None


def create_device_mapping(df_operations):
    """创建设备ID到地点的映射，支持多种匹配方式"""
    mapping = {}

    for _, row in df_operations.iterrows():
        original_device_id = str(row['摄像头编号']).strip()
        location = row['采集地点']

        # 使用clean_device_id函数来标准化设备ID
        cleaned_device_id = clean_device_id(original_device_id)
        if cleaned_device_id:
            mapping[cleaned_device_id] = location

        # 同时添加原始ID的各种变体以提高匹配率
        device_id_lower = original_device_id.lower()
        mapping[device_id_lower] = location

        # 如果包含"-"，添加各个部分
        if '-' in device_id_lower:
            parts = device_id_lower.split('-')
            # 添加前缀
            mapping[parts[0]] = location
            # 添加后缀
            mapping[parts[-1]] = location
            # 添加完整ID（去掉"-"）
            mapping[device_id_lower.replace('-', '')] = location

    return mapping



def read_csv_safe(file_path):
    """安全读取CSV文件（自动尝试不同编码）"""
    try:
        return pd.read_csv(file_path, encoding='utf-8-sig')
    except:
        try:
            return pd.read_csv(file_path, encoding='gbk')
        except:
            return pd.read_csv(file_path, encoding='utf-8')


# ==================== 主流程 ====================

def main():
    print("=" * 80)
    print("数据处理标准化流程")
    print("=" * 80)
    print()
    
    # ========== 步骤1: 合并Operations数据 ==========
    print("【步骤1】合并Operations数据（01.csv, 02.csv, 03.csv）")
    print("-" * 80)
    
    # 读取三个CSV文件
    print("  读取文件...")
    df_01 = read_csv_safe('01.csv')
    df_02 = read_csv_safe('02.csv')
    df_03 = read_csv_safe('03.csv')
    
    print(f"  ✓ 01.csv: {len(df_01)} 行")
    print(f"  ✓ 02.csv: {len(df_02)} 行")
    print(f"  ✓ 03.csv: {len(df_03)} 行")
    
    # 统一列名：只保留前5列
    columns_to_keep = [
        '日期', '摄像头编号', '采集地点', '当日采集任务', '采集时长'
    ]
    
    # 对每个DataFrame只保留前5列并统一列名
    df_01 = df_01.iloc[:, :5]
    df_02 = df_02.iloc[:, :5]
    df_03 = df_03.iloc[:, :5]
    
    df_01.columns = columns_to_keep
    df_02.columns = columns_to_keep
    df_03.columns = columns_to_keep
    
    # 垂直合并三个表
    df_operations = pd.concat([df_01, df_02, df_03], ignore_index=True)
    
    # 清理数据：去除完全空白的行
    df_operations = df_operations.dropna(how='all')
    
    # 统一日期格式并排序
    df_operations['日期_排序'] = df_operations['日期'].apply(clean_date)
    df_operations = df_operations.sort_values('日期_排序', ascending=True).reset_index(drop=True)
    df_operations = df_operations.drop('日期_排序', axis=1)
    
    # 保存中间文件
    df_operations.to_csv('Operations.csv', index=False, encoding='utf-8-sig')
    
    print(f"  ✓ 合并完成: {len(df_operations)} 行")
    print(f"  ✓ 已保存: Operations.csv")
    print()
    
    # ========== 步骤2: 读取QA数据 ==========
    print("【步骤2】读取QA数据")
    print("-" * 80)
    
    df_qa = read_csv_safe('QA.csv')
    print(f"  ✓ QA.csv: {len(df_qa)} 行")
    print()
    
    # ========== 步骤3: 数据清洗 ==========
    print("【步骤3】数据清洗")
    print("-" * 80)
    
    # Operations表处理
    df_operations['标准日期'] = df_operations['日期'].apply(clean_date)
    df_operations['标准设备ID'] = df_operations['摄像头编号'].apply(clean_device_id)
    df_operations['采集时长_数值'] = pd.to_numeric(df_operations['采集时长'], errors='coerce')
    
    # QA表处理
    df_qa['标准日期'] = df_qa['采集日期'].apply(clean_date)
    df_qa['标准设备ID'] = df_qa['设备ID'].apply(clean_device_id)
    df_qa['原始上送时长_数值'] = pd.to_numeric(df_qa['原始上送时长'], errors='coerce')
    
    # 检查字段名（兼容新旧格式）
    if '无效时长' in df_qa.columns:
        df_qa['运营端不合格时长_数值'] = pd.to_numeric(df_qa['无效时长'], errors='coerce')
    elif '运营端不合格时长' in df_qa.columns:
        df_qa['运营端不合格时长_数值'] = pd.to_numeric(df_qa['运营端不合格时长'], errors='coerce')
    else:
        df_qa['运营端不合格时长_数值'] = 0
    
    if '算法端可接受时长' in df_qa.columns:
        df_qa['算法端可接受时长_数值'] = pd.to_numeric(df_qa['算法端可接受时长'], errors='coerce')
    
    print(f"  ✓ Operations表 - 日期缺失: {df_operations['标准日期'].isna().sum()} 条")
    print(f"  ✓ Operations表 - 设备ID缺失: {df_operations['标准设备ID'].isna().sum()} 条")
    print(f"  ✓ QA表 - 日期缺失: {df_qa['标准日期'].isna().sum()} 条")
    print(f"  ✓ QA表 - 设备ID缺失: {df_qa['标准设备ID'].isna().sum()} 条")
    print()
    
    # ========== 步骤4: 创建映射和聚合 ==========
    print("【步骤4】创建映射和聚合数据")
    print("-" * 80)
    
    # 创建设备ID到地点的映射
    device_location_map = create_device_mapping(df_operations)
    print(f"  ✓ 创建了 {len(device_location_map)} 个设备ID映射")
    
    # 聚合Operations数据（按日期和设备ID）
    df_ops_agg = df_operations.groupby(['标准日期', '标准设备ID']).agg({
        '采集时长_数值': 'sum'
    }).reset_index()
    df_ops_agg.rename(columns={'采集时长_数值': '日报截屏上送时长'}, inplace=True)
    print(f"  ✓ Operations表聚合: {len(df_ops_agg)} 条")
    
    # 聚合QA数据（按日期和设备ID）
    df_qa_valid = df_qa[df_qa['标准日期'].notna() & df_qa['标准设备ID'].notna()].copy()
    
    agg_dict = {
        '采集日期': 'first',
        '设备ID': 'first',
        '原始上送时长_数值': 'sum',
        '运营端不合格时长_数值': 'sum',
        '任务描述': 'first'
    }
    
    if '算法端可接受时长_数值' in df_qa_valid.columns:
        agg_dict['算法端可接受时长_数值'] = 'sum'
    
    # 添加NOTE字段的聚合（合并所有NOTE，用分号分隔）
    if 'NOTE' in df_qa_valid.columns:
        agg_dict['NOTE'] = lambda x: '; '.join([str(note) for note in x.dropna() if str(note).strip() != '' and str(note).lower() != 'nan'])
    
    df_qa_agg = df_qa_valid.groupby(['标准日期', '标准设备ID']).agg(agg_dict).reset_index()
    print(f"  ✓ QA表聚合: {len(df_qa_agg)} 条（从 {len(df_qa)} 条聚合而来）")
    print()
    
    # ========== 步骤5: 添加地点和关联数据 ==========
    print("【步骤5】添加地点信息和关联数据")
    print("-" * 80)
    
    # 为聚合后的QA表添加地点
    df_qa_agg['采集地点'] = df_qa_agg['标准设备ID'].map(device_location_map)
    matched_count = df_qa_agg['采集地点'].notna().sum()
    print(f"  ✓ 成功匹配地点: {matched_count} 条")
    
    # 关联数据：使用outer join，保留两个表中的所有记录
    df_merged = df_ops_agg.merge(df_qa_agg, on=['标准日期', '标准设备ID'], how='outer')
    
    # 对于只在Operations表中的记录，补充基础信息
    for idx, row in df_merged.iterrows():
        if pd.isna(row['采集日期']):
            # 这条记录只在Operations表中，补充日期和设备ID
            df_merged.at[idx, '采集日期'] = row['标准日期']
            df_merged.at[idx, '设备ID'] = row['标准设备ID']
    
    print(f"  ✓ 关联完成: {len(df_merged)} 条")
    print(f"  ✓ 只在Operations表: {df_merged['原始上送时长_数值'].isna().sum()} 条")
    print(f"  ✓ 只在QA表: {df_merged['日报截屏上送时长'].isna().sum()} 条")
    print()
    
    # ========== 步骤6: 构建输出表 ==========
    print("【步骤6】构建输出表")
    print("-" * 80)
    
    df_output = pd.DataFrame()
    
    # 基础字段
    df_output['采集日期'] = df_merged['采集日期']
    df_output['设备ID'] = df_merged['设备ID']
    
    # 采集地点 - 优先使用已有的地点，如果没有则从映射中获取
    df_output['采集地点'] = df_merged['采集地点']
    for idx, row in df_output.iterrows():
        if pd.isna(row['采集地点']):
            std_device = df_merged.loc[idx, '标准设备ID']
            if std_device in device_location_map:
                df_output.at[idx, '采集地点'] = device_location_map[std_device]
    
    # 日报截屏上送时长（小时）- 来自Operations表
    df_output['日报截屏上送时长（小时）'] = df_merged['日报截屏上送时长'].fillna(0).round(2)
    
    # 原始上送时长 - 来自QA表聚合（先小时后分钟）
    df_output['原始上送时长（小时）'] = (df_merged['原始上送时长_数值'] / 60).fillna(0).round(2)
    df_output['原始上送时长（分钟）'] = df_merged['原始上送时长_数值'].fillna(0).round(2)
    
    # 运营端不合格时长 - 来自QA表聚合（先小时后分钟）
    df_output['运营端不合格时长（小时）'] = (df_merged['运营端不合格时长_数值'] / 60).fillna(0).round(2)
    df_output['运营端不合格时长（分钟）'] = df_merged['运营端不合格时长_数值'].fillna(0).round(2)
    
    # 算法端可接受时长
    if '算法端可接受时长_数值' in df_merged.columns:
        df_output['算法端可接受时长（分钟）'] = df_merged['算法端可接受时长_数值'].fillna(0).round(2)
        df_output['算法端可接受时长（小时）'] = (df_merged['算法端可接受时长_数值'] / 60).fillna(0).round(2)
    
    # 计算不合格时长占比
    def calc_ratio(row):
        total = row['原始上送时长（分钟）']
        unqualified = row['运营端不合格时长（分钟）']
        if total > 0:
            ratio = (unqualified / total) * 100
            return f"{round(ratio, 2)}%"
        return "0%"
    
    df_output['不合格时长占比'] = df_output.apply(calc_ratio, axis=1)
    
    # 数据记录差异 = 原始上送时长（小时） - 日报截屏上送时长（小时）
    df_output['数据记录差异'] = (
        df_output['原始上送时长（小时）'] - df_output['日报截屏上送时长（小时）']
    ).round(2)
    
    # 任务描述
    df_output['任务描述'] = df_merged['任务描述']
    
    # NOTE字段
    if 'NOTE' in df_merged.columns:
        df_output['NOTE'] = df_merged['NOTE'].fillna('')
    
    # 按日期和设备ID排序（将日期转换为datetime进行排序）
    df_output['日期排序'] = pd.to_datetime(df_output['采集日期'], errors='coerce')
    df_output = df_output.sort_values(['日期排序', '设备ID'], ascending=[True, True]).reset_index(drop=True)
    df_output = df_output.drop('日期排序', axis=1)
    
    # 统计数据来源
    only_ops = (df_output['原始上送时长（小时）'] == 0) & (df_output['日报截屏上送时长（小时）'] > 0)
    only_qa = (df_output['原始上送时长（小时）'] > 0) & (df_output['日报截屏上送时长（小时）'] == 0)
    both = (df_output['原始上送时长（小时）'] > 0) & (df_output['日报截屏上送时长（小时）'] > 0)
    
    print(f"  ✓ 输出表构建完成: {len(df_output)} 条记录")
    print(f"    - 两表都有数据: {both.sum()} 条")
    print(f"    - 只在Operations表: {only_ops.sum()} 条")
    print(f"    - 只在QA表: {only_qa.sum()} 条")
    print()
    
    # ========== 步骤7: 保存结果 ==========
    print("【步骤7】保存结果")
    print("-" * 80)
    
    output_file = 'result.csv'
    df_output.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"  ✓ 聚合报表已保存: {output_file}")
    print()
    
    # ========== 步骤8: 统计NOTE分类 ==========
    print("【步骤8】统计NOTE分类")
    print("-" * 80)
    
    if 'NOTE' in df_output.columns:
        # 统计NOTE的分类
        note_stats = {}
        total_records = len(df_output)
        
        for idx, row in df_output.iterrows():
            note = str(row['NOTE']).strip()
            
            if note == '' or note.lower() == 'nan':
                note_category = '合格'
            else:
                # 根据NOTE内容分类
                note_lower = note.lower()
                if '合格' in note_lower or '可接收' in note_lower:
                    note_category = '合格'
                elif '不合格' in note_lower:
                    note_category = '不合格'
                else:
                    note_category = '其他因素'
            
            note_stats[note_category] = note_stats.get(note_category, 0) + 1
        
        print("  NOTE分类统计:")
        for category, count in sorted(note_stats.items()):
            percentage = (count / total_records) * 100
            print(f"    - {category}: {count} 条 ({percentage:.2f}%)")
        print()
    else:
        print("  ⚠️  QA表中没有NOTE字段")
        print()
    
    # ========== 统计摘要 ==========
    print("=" * 80)
    print("✓ 数据处理完成！")
    print("=" * 80)
    print()
    print("📊 数据统计:")
    print(f"  - 原始QA数据: {len(df_qa)} 条")
    print(f"  - 聚合后记录: {len(df_output)} 条")
    print(f"  - 有地点信息: {matched_count} 条 ({round(matched_count/len(df_output)*100, 1)}%)")
    print(f"  - 无地点信息: {len(df_output) - matched_count} 条")
    print()
    print("📁 生成的文件:")
    print(f"  1. Operations.csv (中间文件 - {len(df_operations)} 行)")
    print(f"  2. result.csv (聚合报表 - {len(df_output)} 行，按日期+设备ID聚合)")
    print()
    print("前5行预览:")
    print(df_output.head(5).to_string())
    print()


if __name__ == '__main__':
    main()
