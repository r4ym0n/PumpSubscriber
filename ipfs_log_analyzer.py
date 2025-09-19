#!/usr/bin/env python3
"""
IPFS Gateway Performance Log Analyzer

分析rs_subscriber产生的IPFS网关性能日志，主要关注ipfs_pull_done事件。
提供网关性能对比、时序分析、win rate计算等功能。
"""

import json
import re
import sys
from datetime import datetime
from collections import defaultdict, Counter
from typing import Dict, List, Tuple, Optional
import statistics
import argparse


class IPFSLogEntry:
    """IPFS日志条目数据结构"""
    
    def __init__(self, timestamp: str, data: dict):
        self.timestamp = timestamp
        self.data = data
        self.bytes = data.get('bytes', 0)
        self.elapsed_ms = data.get('elapsed_ms', 0)
        self.gateway = data.get('gateway', '')
        self.speed_kbps = data.get('speed_kbps', 0)
        self.mint = data.get('mint', '')
        self.subject = data.get('subject', '')
        
    @property
    def datetime(self) -> datetime:
        """解析时间戳为datetime对象"""
        try:
            return datetime.strptime(self.timestamp, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            try:
                return datetime.strptime(self.timestamp, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return datetime.now()


class IPFSLogAnalyzer:
    """IPFS日志分析器"""
    
    def __init__(self):
        self.entries: List[IPFSLogEntry] = []
        self.gateway_stats: Dict[str, dict] = defaultdict(lambda: {
            'total_requests': 0,
            'total_bytes': 0,
            'total_elapsed_ms': 0,
            'elapsed_times': [],
            'speeds': [],
            'file_sizes': []
        })
        
    def parse_log_line(self, line: str) -> Optional[IPFSLogEntry]:
        """解析单行日志"""
        line = line.strip()
        if not line:
            return None
            
        # 处理Docker日志格式，移除容器前缀
        # 格式: rs-subscriber  | [2025-09-19 01:05:52.073] {"bytes":115303,"elapsed_ms":1154,...}
        # 或者: [2025-09-19 01:05:52.073] {"bytes":115303,"elapsed_ms":1154,...}
        
        # 移除Docker容器前缀（如果存在）
        docker_prefix_pattern = r'^[^|]*\|\s*'
        line = re.sub(docker_prefix_pattern, '', line)
        
        # 提取时间戳和JSON数据
        match = re.match(r'\[([^\]]+)\]\s*(.+)', line)
        if not match:
            return None
            
        timestamp_str = match.group(1)
        json_str = match.group(2)
        
        try:
            data = json.loads(json_str)
            # 只处理ipfs_pull_done事件
            if data.get('event') == 'ipfs_pull_done':
                return IPFSLogEntry(timestamp_str, data)
        except json.JSONDecodeError:
            pass
            
        return None
    
    def load_log_file(self, file_path: str):
        """加载日志文件"""
        print(f"正在加载日志文件: {file_path}")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                entry = self.parse_log_line(line)
                if entry:
                    self.entries.append(entry)
                    
        print(f"成功解析 {len(self.entries)} 条 ipfs_pull_done 记录")
        
    def load_log_from_stdin(self):
        """从标准输入加载日志"""
        print("正在从标准输入读取日志...")
        
        for line_num, line in enumerate(sys.stdin, 1):
            entry = self.parse_log_line(line)
            if entry:
                self.entries.append(entry)
                
        print(f"成功解析 {len(self.entries)} 条 ipfs_pull_done 记录")
    
    def calculate_stats(self):
        """计算统计信息"""
        print("正在计算统计信息...")
        
        for entry in self.entries:
            gateway = entry.gateway
            stats = self.gateway_stats[gateway]
            
            stats['total_requests'] += 1
            stats['total_bytes'] += entry.bytes
            stats['total_elapsed_ms'] += entry.elapsed_ms
            stats['elapsed_times'].append(entry.elapsed_ms)
            stats['speeds'].append(entry.speed_kbps)
            stats['file_sizes'].append(entry.bytes)
    
    def calculate_latency_distribution(self, elapsed_times: List[int]) -> Dict[str, dict]:
        """计算耗时分布统计"""
        # 定义耗时区间 (毫秒)
        buckets = [
            ('0-100ms', 0, 100),
            ('100-500ms', 100, 500),
            ('500-2000ms', 500, 2000),
            ('2000ms+', 2000, float('inf'))
        ]
        
        distribution = {}
        total_count = len(elapsed_times)
        
        for bucket_name, min_val, max_val in buckets:
            if max_val == float('inf'):
                count = sum(1 for t in elapsed_times if t >= min_val)
            else:
                count = sum(1 for t in elapsed_times if min_val <= t < max_val)
            
            percentage = (count / total_count * 100) if total_count > 0 else 0
            
            distribution[bucket_name] = {
                'count': count,
                'percentage': percentage,
                'range': f"{min_val}-{max_val if max_val != float('inf') else '∞'}ms"
            }
        
        return distribution

    def get_gateway_summary(self) -> Dict[str, dict]:
        """获取网关汇总统计"""
        summary = {}
        
        for gateway, stats in self.gateway_stats.items():
            if stats['total_requests'] == 0:
                continue
                
            elapsed_times = stats['elapsed_times']
            speeds = stats['speeds']
            file_sizes = stats['file_sizes']
            
            # 计算耗时分布
            latency_distribution = self.calculate_latency_distribution(elapsed_times)
            
            summary[gateway] = {
                'total_requests': stats['total_requests'],
                'total_bytes': stats['total_bytes'],
                'avg_elapsed_ms': stats['total_elapsed_ms'] / stats['total_requests'],
                'median_elapsed_ms': statistics.median(elapsed_times),
                'min_elapsed_ms': min(elapsed_times),
                'max_elapsed_ms': max(elapsed_times),
                'std_elapsed_ms': statistics.stdev(elapsed_times) if len(elapsed_times) > 1 else 0,
                'avg_speed_kbps': statistics.mean(speeds),
                'median_speed_kbps': statistics.median(speeds),
                'avg_file_size_bytes': statistics.mean(file_sizes),
                'total_data_mb': stats['total_bytes'] / (1024 * 1024),
                'latency_distribution': latency_distribution
            }
            
        return summary
    
    def calculate_win_rates(self) -> Dict[str, dict]:
        """计算网关win rate（比其他网关快的次数）"""
        print("正在计算网关win rate...")
        
        # 按文件分组（使用mint+subject作为文件标识）
        file_groups = defaultdict(list)
        for entry in self.entries:
            file_key = f"{entry.mint}_{entry.subject}"
            file_groups[file_key].append(entry)
        
        gateway_wins = Counter()
        gateway_participations = Counter()
        comparison_count = 0
        
        for file_key, entries in file_groups.items():
            if len(entries) < 2:  # 需要至少2个网关的数据才能比较
                continue
                
            # 找出最快的网关
            fastest_entry = min(entries, key=lambda x: x.elapsed_ms)
            fastest_gateway = fastest_entry.gateway
            
            # 记录参与比较的网关
            gateways_in_comparison = set(entry.gateway for entry in entries)
            for gateway in gateways_in_comparison:
                gateway_participations[gateway] += 1
            
            # 记录获胜
            gateway_wins[fastest_gateway] += 1
            comparison_count += 1
        
        # 计算win rate
        win_rates = {}
        for gateway in gateway_participations:
            wins = gateway_wins.get(gateway, 0)
            participations = gateway_participations[gateway]
            win_rate = wins / participations if participations > 0 else 0
            
            win_rates[gateway] = {
                'wins': wins,
                'participations': participations,
                'win_rate': win_rate,
                'win_percentage': win_rate * 100
            }
        
        print(f"分析了 {comparison_count} 个文件的网关对比")
        return win_rates
    
    def simulate_best_gateway_scenario(self) -> dict:
        """模拟总是使用最快网关的场景"""
        print("正在模拟最优网关选择场景...")
        
        # 按文件分组
        file_groups = defaultdict(list)
        for entry in self.entries:
            file_key = f"{entry.mint}_{entry.subject}"
            file_groups[file_key].append(entry)
        
        total_optimal_time = 0
        total_actual_time = 0
        optimal_selections = Counter()
        files_analyzed = 0
        
        for file_key, entries in file_groups.items():
            if len(entries) < 2:
                continue
                
            # 找出最快和实际耗时
            fastest_entry = min(entries, key=lambda x: x.elapsed_ms)
            actual_total_time = sum(entry.elapsed_ms for entry in entries)
            
            total_optimal_time += fastest_entry.elapsed_ms
            total_actual_time += actual_total_time
            optimal_selections[fastest_entry.gateway] += 1
            files_analyzed += 1
        
        time_saved = total_actual_time - total_optimal_time
        efficiency_gain = (time_saved / total_actual_time * 100) if total_actual_time > 0 else 0
        
        return {
            'files_analyzed': files_analyzed,
            'total_optimal_time_ms': total_optimal_time,
            'total_actual_time_ms': total_actual_time,
            'time_saved_ms': time_saved,
            'efficiency_gain_percentage': efficiency_gain,
            'optimal_gateway_selections': dict(optimal_selections),
            'avg_optimal_time_per_file_ms': total_optimal_time / files_analyzed if files_analyzed > 0 else 0,
            'avg_actual_time_per_file_ms': total_actual_time / files_analyzed if files_analyzed > 0 else 0
        }
    
    def analyze_time_series(self) -> dict:
        """分析时序上的耗时变化"""
        print("正在分析时序变化...")
        
        # 按时间排序
        sorted_entries = sorted(self.entries, key=lambda x: x.datetime)
        
        if len(sorted_entries) < 2:
            return {}
        
        # 按小时分组分析
        hourly_stats = defaultdict(lambda: defaultdict(list))
        
        for entry in sorted_entries:
            hour_key = entry.datetime.strftime("%Y-%m-%d %H:00")
            hourly_stats[hour_key][entry.gateway].append(entry.elapsed_ms)
        
        # 计算每小时的统计
        time_series = {}
        for hour, gateways in hourly_stats.items():
            hour_stats = {}
            for gateway, times in gateways.items():
                hour_stats[gateway] = {
                    'count': len(times),
                    'avg_ms': statistics.mean(times),
                    'median_ms': statistics.median(times),
                    'min_ms': min(times),
                    'max_ms': max(times)
                }
            time_series[hour] = hour_stats
        
        return time_series
    
    def add_overall_distribution_analysis(self, report: List[str], summary: Dict[str, dict]):
        """添加整体耗时分布对比分析"""
        # 定义耗时区间
        buckets = ['0-100ms', '100-500ms', '500-2000ms', '2000ms+']
        
        # 创建对比表格
        report.append("各网关耗时分布对比:")
        report.append("")
        
        # 表头
        header = f"{'网关':<30} " + " ".join(f"{bucket:>12}" for bucket in buckets)
        report.append(header)
        report.append("-" * len(header))
        
        # 每个网关的分布数据
        for gateway, stats in summary.items():
            gateway_name = gateway.replace('https://', '').replace('http://', '')
            if len(gateway_name) > 28:
                gateway_name = gateway_name[:25] + "..."
            
            row = f"{gateway_name:<30} "
            latency_dist = stats['latency_distribution']
            
            for bucket in buckets:
                if bucket in latency_dist:
                    count = latency_dist[bucket]['count']
                    percentage = latency_dist[bucket]['percentage']
                    cell = f"{count}({percentage:.0f}%)"
                else:
                    cell = "0(0%)"
                row += f"{cell:>12} "
            
            report.append(row)
        
        report.append("")
        
        # 添加分布分析总结
        report.append("分布分析:")
        
        # 找出快速响应比例最高的网关 (0-500ms)
        fast_response_rates = {}
        for gateway, stats in summary.items():
            dist = stats['latency_distribution']
            fast_count = dist.get('0-100ms', {}).get('count', 0) + dist.get('100-500ms', {}).get('count', 0)
            total_count = stats['total_requests']
            fast_rate = (fast_count / total_count * 100) if total_count > 0 else 0
            fast_response_rates[gateway] = fast_rate
        
        if fast_response_rates:
            best_fast_gateway = max(fast_response_rates.items(), key=lambda x: x[1])
            report.append(f"• 快速响应率最高 (≤500ms): {best_fast_gateway[0]} ({best_fast_gateway[1]:.1f}%)")
        
        # 找出慢响应比例最低的网关 (≥2000ms)
        slow_response_rates = {}
        for gateway, stats in summary.items():
            dist = stats['latency_distribution']
            slow_count = dist.get('2000ms+', {}).get('count', 0)
            total_count = stats['total_requests']
            slow_rate = (slow_count / total_count * 100) if total_count > 0 else 0
            slow_response_rates[gateway] = slow_rate
        
        if slow_response_rates:
            best_slow_gateway = min(slow_response_rates.items(), key=lambda x: x[1])
            report.append(f"• 慢响应率最低 (≥2000ms): {best_slow_gateway[0]} ({best_slow_gateway[1]:.1f}%)")
        
        report.append("")
    
    def generate_report(self) -> str:
        """生成综合分析报告"""
        if not self.entries:
            return "没有找到有效的日志数据"
        
        self.calculate_stats()
        
        report = []
        report.append("=" * 80)
        report.append("IPFS网关性能分析报告")
        report.append("=" * 80)
        report.append("")
        
        # 基本统计
        report.append(f"📊 基本统计信息")
        report.append(f"   总请求数: {len(self.entries)}")
        report.append(f"   分析时间范围: {min(self.entries, key=lambda x: x.datetime).timestamp} 到 {max(self.entries, key=lambda x: x.datetime).timestamp}")
        report.append(f"   涉及网关数: {len(self.gateway_stats)}")
        report.append("")
        
        # 网关性能汇总
        report.append("🚀 网关性能汇总")
        report.append("-" * 60)
        summary = self.get_gateway_summary()
        
        for gateway, stats in sorted(summary.items(), key=lambda x: x[1]['avg_elapsed_ms']):
            report.append(f"网关: {gateway}")
            report.append(f"  请求数: {stats['total_requests']}")
            report.append(f"  平均耗时: {stats['avg_elapsed_ms']:.1f} ms")
            report.append(f"  中位耗时: {stats['median_elapsed_ms']:.1f} ms")
            report.append(f"  耗时范围: {stats['min_elapsed_ms']} - {stats['max_elapsed_ms']} ms")
            report.append(f"  标准差: {stats['std_elapsed_ms']:.1f} ms")
            report.append(f"  平均速度: {stats['avg_speed_kbps']:.1f} KB/s")
            report.append(f"  总数据量: {stats['total_data_mb']:.2f} MB")
            
            # 添加耗时分布统计
            report.append(f"  耗时分布:")
            latency_dist = stats['latency_distribution']
            for bucket_name, bucket_stats in latency_dist.items():
                count = bucket_stats['count']
                percentage = bucket_stats['percentage']
                if count > 0:
                    report.append(f"    {bucket_name}: {count}次 ({percentage:.1f}%)")
            report.append("")
        
        # 整体耗时分布对比
        report.append("📊 整体耗时分布对比")
        report.append("-" * 60)
        self.add_overall_distribution_analysis(report, summary)
        
        # Win Rate分析
        report.append("🏆 网关Win Rate分析")
        report.append("-" * 60)
        win_rates = self.calculate_win_rates()
        
        for gateway, wr_stats in sorted(win_rates.items(), key=lambda x: x[1]['win_rate'], reverse=True):
            report.append(f"网关: {gateway}")
            report.append(f"  获胜次数: {wr_stats['wins']} / {wr_stats['participations']}")
            report.append(f"  Win Rate: {wr_stats['win_percentage']:.1f}%")
            report.append("")
        
        # 最优网关场景分析
        report.append("⚡ 最优网关选择场景分析")
        report.append("-" * 60)
        optimal_scenario = self.simulate_best_gateway_scenario()
        
        if optimal_scenario:
            report.append(f"分析文件数: {optimal_scenario['files_analyzed']}")
            report.append(f"当前总耗时: {optimal_scenario['total_actual_time_ms']} ms")
            report.append(f"最优总耗时: {optimal_scenario['total_optimal_time_ms']} ms")
            report.append(f"可节省时间: {optimal_scenario['time_saved_ms']} ms")
            report.append(f"效率提升: {optimal_scenario['efficiency_gain_percentage']:.1f}%")
            report.append(f"平均单文件耗时 (当前): {optimal_scenario['avg_actual_time_per_file_ms']:.1f} ms")
            report.append(f"平均单文件耗时 (最优): {optimal_scenario['avg_optimal_time_per_file_ms']:.1f} ms")
            report.append("")
            report.append("最优网关选择分布:")
            for gateway, count in optimal_scenario['optimal_gateway_selections'].items():
                percentage = count / optimal_scenario['files_analyzed'] * 100
                report.append(f"  {gateway}: {count} 次 ({percentage:.1f}%)")
            report.append("")
        
        # 时序分析
        report.append("📈 时序分析 (按小时)")
        report.append("-" * 60)
        time_series = self.analyze_time_series()
        
        if time_series:
            for hour in sorted(time_series.keys())[-10:]:  # 显示最近10小时
                report.append(f"时间: {hour}")
                hour_data = time_series[hour]
                for gateway, stats in hour_data.items():
                    report.append(f"  {gateway}: {stats['count']}次, 平均{stats['avg_ms']:.1f}ms")
                report.append("")
        
        # 推荐建议
        report.append("💡 优化建议")
        report.append("-" * 60)
        
        if summary:
            fastest_gateway = min(summary.items(), key=lambda x: x[1]['avg_elapsed_ms'])
            slowest_gateway = max(summary.items(), key=lambda x: x[1]['avg_elapsed_ms'])
            
            report.append(f"1. 性能最佳网关: {fastest_gateway[0]} (平均 {fastest_gateway[1]['avg_elapsed_ms']:.1f}ms)")
            report.append(f"2. 性能最差网关: {slowest_gateway[0]} (平均 {slowest_gateway[1]['avg_elapsed_ms']:.1f}ms)")
            
            if optimal_scenario and optimal_scenario['efficiency_gain_percentage'] > 10:
                report.append(f"3. 建议实施智能网关选择，可提升效率 {optimal_scenario['efficiency_gain_percentage']:.1f}%")
            
            # 分析网关稳定性
            most_stable = min(summary.items(), key=lambda x: x[1]['std_elapsed_ms'])
            report.append(f"4. 最稳定网关: {most_stable[0]} (标准差 {most_stable[1]['std_elapsed_ms']:.1f}ms)")
        
        report.append("")
        report.append("=" * 80)
        
        return "\n".join(report)


def main():
    parser = argparse.ArgumentParser(description='IPFS网关性能日志分析器')
    parser.add_argument('logfile', nargs='?', help='日志文件路径 (如果不提供则从stdin读取)')
    parser.add_argument('--output', '-o', help='输出报告到文件')
    
    args = parser.parse_args()
    
    analyzer = IPFSLogAnalyzer()
    
    try:
        if args.logfile:
            analyzer.load_log_file(args.logfile)
        else:
            analyzer.load_log_from_stdin()
        
        report = analyzer.generate_report()
        
        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                f.write(report)
            print(f"报告已保存到: {args.output}")
        else:
            print(report)
            
    except KeyboardInterrupt:
        print("\n分析被用户中断")
    except Exception as e:
        print(f"错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
