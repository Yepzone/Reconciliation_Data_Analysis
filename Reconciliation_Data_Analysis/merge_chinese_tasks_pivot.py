#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
创建采集点×任务的透视表，只合并中文相似任务，保留英文任务
横轴：采集点，纵轴：任务，数值：总时长
"""

import pandas as pd

def read_csv_safe(file_path):
    """安全读取CSV文件"""
    try:
        return pd.read_csv(file_path, encoding='utf-8-sig')
    except:
        try:
            return pd.read_csv(file_path, encoding='gbk')
        except:
            return pd.read_csv(file_path, encoding='utf-8')

def create_chinese_task_mapping():
    """创建中文任务映射表，只合并中文相似任务"""
    task_mapping = {
        # 格式问题修复（中文）
        '叠衣服 ': '叠衣服',  # 去掉末尾空格
        
        # 语义相似任务合并（中文）
        '书本收纳': '整理书籍',
        '擦拭餐具': '用抹布擦拭碗盘',
        
        # 其他中文任务合并
        '安装电池': '电池安装',
        '整理文档': '整理文件',
        
        # 保留所有英文任务不变
        # 'Sort nuts/beans': 'Sort nuts/beans',  # 保持原样
        # 'Fold shopping bags': 'Fold shopping bags',  # 保持原样
        # 等等...
    }
    return task_mapping

def main():
    print("=" * 80)
    print("创建采集点×任务透视表（只合并中文相似任务）")
    print("=" * 80)
    
    # 1. 读取result.csv
    try:
        df = read_csv_safe('result.csv')
        print(f"读取result.csv: {len(df)} 条记录")
    except FileNotFoundError:
        print("❌ 未找到result.csv")
        return
    
    # 2. 读取设备地点映射表
    try:
        mapping_df = read_csv_safe('设备地点映射表.csv')
        device_location_map = dict(zip(mapping_df['设备ID'], mapping_df['Unnamed: 9']))
        print(f"读取设备地点映射表: {len(device_location_map)} 个设备映射")
    except FileNotFoundError:
        print("⚠️ 未找到设备地点映射表.csv，将使用原有地点信息")
        device_location_map = {}
    
    # 3. 过滤有效记录
    df_valid = df[df['任务描述'].notna() & (df['任务描述'] != '')].copy()
    print(f"有效记录（有任务描述）: {len(df_valid)} 条")
    
    # 4. 更新地点信息
    df_valid['最终地点'] = df_valid['采集地点'].fillna('')
    
    for idx, row in df_valid.iterrows():
        if pd.isna(row['采集地点']) or row['采集地点'] == '':
            device_id = row['设备ID']
            if device_id in device_location_map:
                df_valid.at[idx, '最终地点'] = device_location_map[device_id]
            else:
                df_valid.at[idx, '最终地点'] = '未知'
    
    # 5. 应用中文任务映射
    task_mapping = create_chinese_task_mapping()
    df_valid['标准任务名'] = df_valid['任务描述'].map(task_mapping).fillna(df_valid['任务描述'])
    
    print(f"\n📊 任务合并统计:")
    original_tasks = df_valid['任务描述'].nunique()
    merged_tasks = df_valid['标准任务名'].nunique()
    print(f"  - 原始任务种类: {original_tasks} 种")
    print(f"  - 合并后任务种类: {merged_tasks} 种")
    print(f"  - 减少任务种类: {original_tasks - merged_tasks} 种")
    
    # 显示合并的任务
    print(f"\n🔄 中文任务合并详情:")
    for original, standard in task_mapping.items():
        if original in df_valid['任务描述'].values:
            original_data = df_valid[df_valid['任务描述'] == original]
            original_hours = original_data['原始上送时长（小时）'].sum()
            print(f"  {original} ({original_hours:.2f}h) → {standard}")
    
    # 显示所有任务（合并后）
    all_tasks = sorted(df_valid['标准任务名'].unique())
    print(f"\n📋 所有任务（合并后，{len(all_tasks)} 种）:")
    
    # 分类显示
    chinese_tasks = [task for task in all_tasks if any('\u4e00' <= char <= '\u9fff' for char in task)]
    english_tasks = [task for task in all_tasks if task not in chinese_tasks]
    
    print(f"\n  中文任务 ({len(chinese_tasks)} 种):")
    for i, task in enumerate(chinese_tasks, 1):
        print(f"    {i:2d}. {task}")
    
    print(f"\n  英文任务 ({len(english_tasks)} 种):")
    for i, task in enumerate(english_tasks, 1):
        print(f"    {i:2d}. {task}")
    
    # 6. 创建透视表 - 总时长
    print(f"\n📊 创建透视表...")
    
    pivot_hours = df_valid.pivot_table(
        index='标准任务名',
        columns='最终地点',
        values='原始上送时长（小时）',
        aggfunc='sum',
        fill_value=0
    ).round(2)
    
    # 添加行总计和列总计
    pivot_hours['总计'] = pivot_hours.sum(axis=1)
    pivot_hours.loc['总计'] = pivot_hours.sum(axis=0)
    
    # 按总计排序（除了最后一行）
    pivot_hours_sorted = pivot_hours.iloc[:-1].sort_values('总计', ascending=False)
    pivot_hours_sorted = pd.concat([pivot_hours_sorted, pivot_hours.iloc[[-1]]])
    
    # 保存透视表
    pivot_hours_sorted.to_csv('中文合并任务透视表.csv', encoding='utf-8-sig')
    print(f"✓ 中文合并任务透视表.csv已保存")
    
    # 7. 创建透视表 - 记录数量
    pivot_records = df_valid.pivot_table(
        index='标准任务名',
        columns='最终地点',
        values='采集日期',
        aggfunc='count',
        fill_value=0
    )
    
    # 添加行总计和列总计
    pivot_records['总计'] = pivot_records.sum(axis=1)
    pivot_records.loc['总计'] = pivot_records.sum(axis=0)
    
    # 按总计排序
    pivot_records_sorted = pivot_records.iloc[:-1].sort_values('总计', ascending=False)
    pivot_records_sorted = pd.concat([pivot_records_sorted, pivot_records.iloc[[-1]]])
    
    # 保存记录数透视表
    pivot_records_sorted.to_csv('中文合并任务记录数透视表.csv', encoding='utf-8-sig')
    print(f"✓ 中文合并任务记录数透视表.csv已保存")
    
    # 8. 显示透视表预览
    print(f"\n📊 中文合并任务透视表预览（前15行）:")
    print(pivot_hours_sorted.head(15).to_string())
    
    # 9. 生成统计摘要
    print(f"\n📈 透视表统计摘要:")
    
    # 各地点总时长
    location_totals = pivot_hours_sorted.loc['总计'].drop('总计').sort_values(ascending=False)
    print(f"\n🏢 各采集点总时长排序:")
    for i, (location, hours) in enumerate(location_totals.items(), 1):
        percentage = (hours / location_totals.sum() * 100) if location_totals.sum() > 0 else 0
        print(f"  {i}. {location}: {hours:.2f}小时 ({percentage:.1f}%)")
    
    # 各任务总时长
    task_totals = pivot_hours_sorted['总计'].drop('总计').sort_values(ascending=False)
    print(f"\n🎯 前15个任务总时长排序:")
    for i, (task, hours) in enumerate(task_totals.head(15).items(), 1):
        percentage = (hours / task_totals.sum() * 100) if task_totals.sum() > 0 else 0
        task_type = "中文" if any('\u4e00' <= char <= '\u9fff' for char in task) else "英文"
        print(f"  {i:2d}. {task} ({task_type}): {hours:.2f}小时 ({percentage:.1f}%)")
    
    # 10. 分别统计中英文任务
    chinese_task_totals = {}
    english_task_totals = {}
    
    for task, hours in task_totals.items():
        if any('\u4e00' <= char <= '\u9fff' for char in task):
            chinese_task_totals[task] = hours
        else:
            english_task_totals[task] = hours
    
    print(f"\n📊 中文任务 vs 英文任务统计:")
    chinese_total = sum(chinese_task_totals.values())
    english_total = sum(english_task_totals.values())
    total_all = chinese_total + english_total
    
    print(f"  - 中文任务总时长: {chinese_total:.2f}小时 ({chinese_total/total_all*100:.1f}%)")
    print(f"  - 英文任务总时长: {english_total:.2f}小时 ({english_total/total_all*100:.1f}%)")
    print(f"  - 中文任务种类: {len(chinese_task_totals)} 种")
    print(f"  - 英文任务种类: {len(english_task_totals)} 种")
    
    print(f"\n🔝 前5个中文任务:")
    for i, (task, hours) in enumerate(sorted(chinese_task_totals.items(), key=lambda x: x[1], reverse=True)[:5], 1):
        print(f"  {i}. {task}: {hours:.2f}小时")
    
    print(f"\n🔝 前5个英文任务:")
    for i, (task, hours) in enumerate(sorted(english_task_totals.items(), key=lambda x: x[1], reverse=True)[:5], 1):
        print(f"  {i}. {task}: {hours:.2f}小时")
    
    print(f"\n" + "=" * 80)
    print("✓ 中文合并任务透视表创建完成！")
    print("=" * 80)
    
    print(f"\n📁 生成的文件:")
    print(f"  1. 中文合并任务透视表.csv - 横轴采集点，纵轴任务（中文合并），数值为总时长")
    print(f"  2. 中文合并任务记录数透视表.csv - 横轴采集点，纵轴任务（中文合并），数值为记录数")

if __name__ == '__main__':
    main()