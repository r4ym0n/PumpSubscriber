#!/usr/bin/env python3
"""
Smart IPFS Fetcher 性能分析脚本
分析 smart-fetcher 的日志数据，提供详细的性能统计和数据故事
"""

import json
import sys
import statistics
from collections import defaultdict, Counter
from datetime import datetime
import argparse

def parse_log_line(line):
    """解析日志行，提取JSON数据"""
    try:
        # 查找JSON部分 (在时间戳后的大括号)
        json_start = line.find('{')
        if json_start == -1:
            return None
        
        json_str = line[json_start:]
        return json.loads(json_str)
    except (json.JSONDecodeError, ValueError):
        return None

def analyze_performance(log_file):
    """分析性能数据"""
    
    # 数据收集
    success_events = []
    failed_events = []
    local_success = []
    fallback_success = []
    
    # 网关性能统计
    gateway_stats = defaultdict(list)
    
    # 按小时统计
    hourly_stats = defaultdict(lambda: {'success': 0, 'failed': 0, 'total_time': 0})
    
    print("📊 正在分析日志文件...")
    
    with open(log_file, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            data = parse_log_line(line)
            if not data:
                continue
                
            event = data.get('event', '')
            
            # 收集成功事件
            if event == 'smart_ipfs_fetch_success':
                success_events.append(data)
                
                # 按策略分类
                strategy = data.get('strategy', '')
                if strategy == 'local_only':
                    local_success.append(data)
                elif strategy == 'fallback_to_public':
                    fallback_success.append(data)
                
                # 按小时统计
                try:
                    ts = data.get('ts', '')
                    hour = ts.split(' ')[1].split(':')[0] if ' ' in ts else '00'
                    hourly_stats[hour]['success'] += 1
                    hourly_stats[hour]['total_time'] += data.get('total_elapsed_ms', 0)
                except:
                    pass
            
            # 收集失败事件
            elif event == 'smart_ipfs_fetch_failed':
                failed_events.append(data)
                
                # 按小时统计失败
                try:
                    ts = data.get('ts', '')
                    hour = ts.split(' ')[1].split(':')[0] if ' ' in ts else '00'
                    hourly_stats[hour]['failed'] += 1
                except:
                    pass
            
            # 收集网关性能数据
            elif event == 'smart_ipfs_public_success':
                gateway = data.get('gateway', '')
                elapsed = data.get('elapsed_ms', 0)
                if gateway and elapsed:
                    gateway_stats[gateway].append(elapsed)
    
    return {
        'success_events': success_events,
        'failed_events': failed_events,
        'local_success': local_success,
        'fallback_success': fallback_success,
        'gateway_stats': dict(gateway_stats),
        'hourly_stats': dict(hourly_stats)
    }

def calculate_percentiles(data, percentiles=[50, 75, 90, 95, 99]):
    """计算百分位数"""
    if not data:
        return {p: 0 for p in percentiles}
    
    sorted_data = sorted(data)
    result = {}
    for p in percentiles:
        idx = int(len(sorted_data) * p / 100)
        if idx >= len(sorted_data):
            idx = len(sorted_data) - 1
        result[p] = sorted_data[idx]
    return result

def format_time(ms):
    """格式化时间显示"""
    if ms < 1000:
        return f"{ms}ms"
    elif ms < 60000:
        return f"{ms/1000:.1f}s"
    else:
        return f"{ms/60000:.1f}min"

def format_size(bytes_val):
    """格式化文件大小"""
    if bytes_val < 1024:
        return f"{bytes_val}B"
    elif bytes_val < 1024*1024:
        return f"{bytes_val/1024:.1f}KB"
    else:
        return f"{bytes_val/(1024*1024):.1f}MB"

def print_report(stats):
    """生成详细的分析报告"""
    
    success_events = stats['success_events']
    failed_events = stats['failed_events']
    local_success = stats['local_success']
    fallback_success = stats['fallback_success']
    gateway_stats = stats['gateway_stats']
    hourly_stats = stats['hourly_stats']
    
    total_attempts = len(success_events) + len(failed_events)
    
    print("\n" + "="*80)
    print("🚀 SMART IPFS FETCHER 性能分析报告")
    print("="*80)
    
    if total_attempts == 0:
        print("❌ 没有找到相关的拉取事件数据")
        return
    
    # 1. 总体概况
    print("\n📈 总体概况")
    print("-" * 40)
    success_rate = len(success_events) / total_attempts * 100
    print(f"总拉取次数: {total_attempts:,}")
    print(f"成功次数: {len(success_events):,}")
    print(f"失败次数: {len(failed_events):,}")
    print(f"成功率: {success_rate:.1f}%")
    
    if not success_events:
        print("\n❌ 没有成功的拉取事件，无法进行详细分析")
        return
    
    # 2. 策略效果分析
    print(f"\n🎯 策略效果分析")
    print("-" * 40)
    local_count = len(local_success)
    fallback_count = len(fallback_success)
    
    print(f"本地网关成功: {local_count:,} 次 ({local_count/len(success_events)*100:.1f}%)")
    print(f"回退到公共网关: {fallback_count:,} 次 ({fallback_count/len(success_events)*100:.1f}%)")
    
    # 本地网关成功的平均时间
    if local_success:
        local_times = [event.get('total_elapsed_ms', 0) for event in local_success]
        local_avg = statistics.mean(local_times)
        local_percentiles = calculate_percentiles(local_times)
        print(f"\n📊 本地网关成功统计:")
        print(f"  平均耗时: {format_time(local_avg)}")
        print(f"  中位数: {format_time(local_percentiles[50])}")
        print(f"  95分位数: {format_time(local_percentiles[95])}")
    
    # 回退策略的详细分析
    if fallback_success:
        fallback_times = [event.get('total_elapsed_ms', 0) for event in fallback_success]
        fallback_fetch_times = [event.get('fetch_elapsed_ms', 0) for event in fallback_success if event.get('fetch_elapsed_ms')]
        fallback_avg = statistics.mean(fallback_times)
        fallback_percentiles = calculate_percentiles(fallback_times)
        
        print(f"\n📊 回退策略统计:")
        print(f"  平均总耗时: {format_time(fallback_avg)}")
        print(f"  中位数: {format_time(fallback_percentiles[50])}")
        print(f"  95分位数: {format_time(fallback_percentiles[95])}")
        
        if fallback_fetch_times:
            fetch_avg = statistics.mean(fallback_fetch_times)
            print(f"  平均下载耗时: {format_time(fetch_avg)}")
    
    # 3. 总耗时分布分析
    print(f"\n⏱️  总耗时分布分析")
    print("-" * 40)
    
    all_times = [event.get('total_elapsed_ms', 0) for event in success_events]
    avg_time = statistics.mean(all_times)
    percentiles = calculate_percentiles(all_times)
    p95_time = percentiles[95]
    
    print(f"样本数量: {len(all_times):,}")
    print(f"平均耗时: {format_time(avg_time)}")
    print(f"P95耗时: {format_time(p95_time)}")
    print(f"最短耗时: {format_time(min(all_times))}")
    print(f"最长耗时: {format_time(max(all_times))}")
    print(f"标准差: {format_time(statistics.stdev(all_times) if len(all_times) > 1 else 0)}")
    
    print(f"\n百分位数分布:")
    for p in [50, 75, 90, 95, 99]:
        print(f"  {p}分位数: {format_time(percentiles[p])}")
    
    # 耗时区间分布
    print(f"\n耗时区间分布:")
    ranges = [
        (0, 200, "< 200ms"),
        (200, 500, "200-500ms"), 
        (500, 1000, "500ms-1s"),
        (1000, 2000, "1-2s"),
        (2000, 5000, "2-5s"),
        (5000, float('inf'), "> 5s")
    ]
    
    for min_val, max_val, label in ranges:
        count = sum(1 for t in all_times if min_val <= t < max_val)
        percentage = count / len(all_times) * 100
        bar = "█" * int(percentage / 2)  # 简单的条形图
        print(f"  {label:>10}: {count:4d} ({percentage:5.1f}%) {bar}")
    
    # 4. 网关性能对比
    if gateway_stats:
        print(f"\n🌐 公共网关性能对比")
        print("-" * 40)
        
        gateway_performance = []
        for gateway, times in gateway_stats.items():
            if times:
                avg_time = statistics.mean(times)
                p95_time = calculate_percentiles(times, [95])[95]
                gateway_performance.append((gateway, avg_time, p95_time, len(times)))
        
        # 按平均时间排序
        gateway_performance.sort(key=lambda x: x[1])
        
        print(f"{'网关':<35} {'平均耗时':<10} {'P95耗时':<10} {'次数':<6} {'性能'}")
        print("-" * 80)
        
        for gateway, avg_time, p95_time, count in gateway_performance:
            # 简化网关名称显示
            gateway_name = gateway.replace('https://', '').replace('/ipfs', '')
            if len(gateway_name) > 32:
                gateway_name = gateway_name[:29] + "..."
            
            performance_bar = "★" * min(5, max(1, int(6 - avg_time/1000)))  # 性能星级
            print(f"{gateway_name:<35} {format_time(avg_time):<10} {format_time(p95_time):<10} {count:<6} {performance_bar}")
    
    # 5. 文件大小和速度分析
    print(f"\n📦 文件大小和下载速度分析")
    print("-" * 40)
    
    sizes = [event.get('bytes', 0) for event in success_events if event.get('bytes')]
    speeds = [event.get('speed_kbps', 0) for event in success_events if event.get('speed_kbps')]
    
    if sizes:
        avg_size = statistics.mean(sizes)
        size_percentiles = calculate_percentiles(sizes)
        print(f"平均文件大小: {format_size(avg_size)}")
        print(f"文件大小中位数: {format_size(size_percentiles[50])}")
        print(f"最大文件: {format_size(max(sizes))}")
    
    if speeds:
        avg_speed = statistics.mean(speeds)
        speed_percentiles = calculate_percentiles(speeds)
        print(f"平均下载速度: {avg_speed:.0f} KB/s")
        print(f"速度中位数: {speed_percentiles[50]:.0f} KB/s")
        print(f"最高速度: {max(speeds):.0f} KB/s")
    
    # 6. 按小时统计
    if hourly_stats:
        print(f"\n🕐 按小时活动统计")
        print("-" * 40)
        
        print(f"{'小时':<6} {'成功':<6} {'失败':<6} {'成功率':<8} {'平均耗时'}")
        print("-" * 45)
        
        for hour in sorted(hourly_stats.keys()):
            stats_hour = hourly_stats[hour]
            success = stats_hour['success']
            failed = stats_hour['failed']
            total = success + failed
            
            if total > 0:
                success_rate_hour = success / total * 100
                avg_time_hour = stats_hour['total_time'] / success if success > 0 else 0
                print(f"{hour}:00  {success:<6} {failed:<6} {success_rate_hour:6.1f}%  {format_time(avg_time_hour)}")
    
    # 7. 数据故事总结
    print(f"\n📖 数据故事总结")
    print("-" * 40)
    
    # 计算一些关键指标
    fast_requests = sum(1 for t in all_times if t < 500)  # 500ms以下
    slow_requests = sum(1 for t in all_times if t > 2000)  # 2秒以上
    
    print(f"🎯 关键发现:")
    print(f"  • 在 {len(success_events):,} 次成功拉取中，{local_count:,} 次({local_count/len(success_events)*100:.1f}%)通过本地网关完成")
    print(f"  • {fallback_count:,} 次需要回退到公共网关，说明本地网关的可用性有待提升")
    print(f"  • {fast_requests:,} 次拉取在500ms内完成({fast_requests/len(success_events)*100:.1f}%)，用户体验良好")
    
    if slow_requests > 0:
        print(f"  • {slow_requests:,} 次拉取超过2秒({slow_requests/len(success_events)*100:.1f}%)，需要关注慢请求优化")
    
    print(f"  • 平均拉取时间为 {format_time(avg_time)}，P95拉取时间为 {format_time(percentiles[95])}，整体性能{'良好' if avg_time < 1000 else '一般' if avg_time < 2000 else '需要优化'}")
    
    if gateway_performance:
        best_gateway = gateway_performance[0][0].replace('https://', '').replace('/ipfs', '')
        print(f"  • 最快的公共网关是 {best_gateway}，平均响应时间 {format_time(gateway_performance[0][1])}，P95响应时间 {format_time(gateway_performance[0][2])}")
    
    print(f"\n💡 优化建议:")
    if local_count / len(success_events) < 0.5:
        print(f"  • 考虑优化本地IPFS网关配置，提高本地成功率")
    if avg_time > 1000:
        print(f"  • 平均耗时较长，建议检查网络连接和网关配置")
    if slow_requests / len(success_events) > 0.1:
        print(f"  • 超过10%的请求较慢，建议增加更多高性能网关")
    
    print("\n" + "="*80)

def main():
    parser = argparse.ArgumentParser(description='Smart IPFS Fetcher 性能分析工具')
    parser.add_argument('log_file', help='日志文件路径')
    parser.add_argument('--json', action='store_true', help='输出JSON格式的统计数据')
    
    args = parser.parse_args()
    
    try:
        stats = analyze_performance(args.log_file)
        
        if args.json:
            # 输出JSON格式的统计数据
            all_times = [event.get('total_elapsed_ms', 0) for event in stats['success_events']]
            overall_p95 = calculate_percentiles(all_times, [95])[95] if all_times else 0
            overall_avg = statistics.mean(all_times) if all_times else 0
            
            json_stats = {
                'total_success': len(stats['success_events']),
                'total_failed': len(stats['failed_events']),
                'local_success_count': len(stats['local_success']),
                'fallback_success_count': len(stats['fallback_success']),
                'overall_avg_time': overall_avg,
                'overall_p95_time': overall_p95,
                'gateway_stats': {k: {
                    'count': len(v), 
                    'avg_time': statistics.mean(v) if v else 0,
                    'p95_time': calculate_percentiles(v, [95])[95] if v else 0
                } for k, v in stats['gateway_stats'].items()}
            }
            print(json.dumps(json_stats, indent=2, ensure_ascii=False))
        else:
            print_report(stats)
            
    except FileNotFoundError:
        print(f"❌ 错误: 找不到日志文件 '{args.log_file}'")
        sys.exit(1)
    except Exception as e:
        print(f"❌ 分析过程中出现错误: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
