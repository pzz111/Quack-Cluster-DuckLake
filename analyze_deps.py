#!/usr/bin/env python3
"""Analyze task dependencies from implementation plan."""

from collections import defaultdict, deque
from typing import Dict, List, Set, Tuple

# Task data extracted from YAML
tasks_data = [
    ("001", "创建 WorkerError 异常类", "impl", []),
    ("002", "资源管理 - DuckDB 连接上下文管理器测试", "test", []),
    ("003", "资源管理 - DuckDB 连接上下文管理器实现", "impl", ["002"]),
    ("004", "资源管理 - 临时表上下文管理器测试", "test", []),
    ("005", "资源管理 - 临时表上下文管理器实现", "impl", ["004"]),
    ("006", "错误处理 - Worker 错误传播测试", "test", ["001"]),
    ("007", "错误处理 - Worker 错误传播实现", "impl", ["001", "006"]),
    ("008", "错误处理 - 空结果区分测试", "test", ["001"]),
    ("009", "错误处理 - 空结果区分实现", "impl", ["001", "008"]),
    ("010", "并发安全 - 缓存原子操作测试", "test", []),
    ("011", "并发安全 - 缓存原子操作实现", "impl", ["010"]),
    ("012", "容错性 - 指数退避重试测试", "test", ["001"]),
    ("013", "容错性 - 指数退避重试实现", "impl", ["001", "012"]),
    ("014", "容错性 - 查询超时测试", "test", []),
    ("015", "容错性 - 查询超时实现", "impl", ["014"]),
    ("016", "文件验证 - 文件存在性检查测试", "test", []),
    ("017", "文件验证 - 文件存在性检查实现", "impl", ["016"]),
    ("018", "集成测试 - 端到端资源管理验证", "test", ["003", "005"]),
    ("019", "集成测试 - 端到端错误处理验证", "test", ["007", "009"]),
    ("020", "集成测试 - 端到端容错性验证", "test", ["013", "015"]),
    ("021", "配置更新 - 添加重试和超时配置", "config", ["013", "015"]),
    ("022", "文档更新 - 更新架构和开发指南", "docs", ["003", "005", "007", "009", "011", "013", "015", "017"]),
]

def build_graphs():
    """Build dependency graphs."""
    task_info = {}
    graph = defaultdict(list)  # task -> dependents
    reverse_graph = defaultdict(list)  # task -> dependencies

    for task_id, subject, task_type, deps in tasks_data:
        task_info[task_id] = {
            'subject': subject,
            'type': task_type,
            'depends_on': deps
        }
        for dep in deps:
            graph[dep].append(task_id)
            reverse_graph[task_id].append(dep)

    return task_info, graph, reverse_graph

def detect_cycles(task_info, graph):
    """Detect circular dependencies using DFS."""
    WHITE, GRAY, BLACK = 0, 1, 2
    color = {tid: WHITE for tid in task_info.keys()}

    def dfs(node, path):
        if color[node] == GRAY:
            cycle_start = path.index(node)
            return path[cycle_start:] + [node]
        if color[node] == BLACK:
            return None

        color[node] = GRAY
        path.append(node)

        for neighbor in graph[node]:
            result = dfs(neighbor, path[:])
            if result:
                return result

        color[node] = BLACK
        return None

    for task_id in task_info.keys():
        if color[task_id] == WHITE:
            cycle = dfs(task_id, [])
            if cycle:
                return cycle
    return None

def check_red_green_pattern(task_info):
    """Check if impl tasks follow RED-GREEN pattern."""
    violations = []
    for task_id, info in task_info.items():
        if info['type'] == 'impl':
            deps = info['depends_on']
            has_test_dep = any(task_info[dep]['type'] == 'test' for dep in deps)

            # Exception: Task 001 is infrastructure
            if not has_test_dep and task_id != '001':
                violations.append((task_id, info['subject']))

    return violations

def check_missing_dependencies(task_info):
    """Identify missing dependencies."""
    issues = []

    # Check if impl tasks using WorkerError depend on 001
    error_related_impls = ['007', '009', '013']
    for task_id in error_related_impls:
        if '001' not in task_info[task_id]['depends_on']:
            issues.append(f"Task {task_id} missing dependency on 001 (WorkerError)")

    # Verify integration test dependencies
    integration_expected = {
        '018': (['003', '005'], 'resource management'),
        '019': (['007', '009'], 'error handling'),
        '020': (['013', '015'], 'fault tolerance')
    }

    for task_id, (expected, category) in integration_expected.items():
        actual = set(task_info[task_id]['depends_on'])
        expected_set = set(expected)
        if actual != expected_set:
            issues.append(f"Task {task_id} ({category}): expected {expected_set}, got {actual}")

    return issues

def topological_sort(task_info, graph):
    """Generate topological execution order."""
    in_degree = {tid: len(task_info[tid]['depends_on']) for tid in task_info.keys()}
    queue = deque([tid for tid in sorted(task_info.keys()) if in_degree[tid] == 0])
    result = []

    while queue:
        node = queue.popleft()
        result.append(node)

        for neighbor in sorted(graph[node]):
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    return result

def generate_mermaid(task_info, graph):
    """Generate Mermaid dependency graph."""
    lines = ["graph TD"]

    # Add nodes with styling
    for task_id, info in task_info.items():
        task_type = info['type']
        subject = info['subject'][:30] + "..." if len(info['subject']) > 30 else info['subject']

        if task_type == 'test':
            lines.append(f'    {task_id}["{task_id}: {subject}"]:::testNode')
        elif task_type == 'impl':
            lines.append(f'    {task_id}["{task_id}: {subject}"]:::implNode')
        elif task_type == 'config':
            lines.append(f'    {task_id}["{task_id}: {subject}"]:::configNode')
        else:  # docs
            lines.append(f'    {task_id}["{task_id}: {subject}"]:::docsNode')

    # Add edges
    for task_id, info in task_info.items():
        for dep in info['depends_on']:
            lines.append(f'    {dep} --> {task_id}')

    # Add styling
    lines.append('')
    lines.append('    classDef testNode fill:#e1f5ff,stroke:#01579b,stroke-width:2px')
    lines.append('    classDef implNode fill:#fff3e0,stroke:#e65100,stroke-width:2px')
    lines.append('    classDef configNode fill:#f3e5f5,stroke:#4a148c,stroke-width:2px')
    lines.append('    classDef docsNode fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px')

    return '\n'.join(lines)

def main():
    """Main analysis function."""
    task_info, graph, reverse_graph = build_graphs()

    print("=" * 80)
    print("TASK DEPENDENCY ANALYSIS REPORT")
    print("=" * 80)

    # 1. Circular Dependencies
    print("\n1. CIRCULAR DEPENDENCIES")
    print("-" * 80)
    cycle = detect_cycles(task_info, graph)
    if cycle:
        print(f"VIOLATION FOUND: {' -> '.join(cycle)}")
    else:
        print("Status: PASS - No circular dependencies detected")

    # 2. RED-GREEN Pattern
    print("\n2. RED-GREEN PATTERN VALIDATION")
    print("-" * 80)
    violations = check_red_green_pattern(task_info)
    if violations:
        print("VIOLATIONS FOUND:")
        for task_id, subject in violations:
            print(f"  - Task {task_id}: {subject}")
            print(f"    Issue: Implementation without preceding test dependency")
    else:
        print("Status: PASS - All impl tasks follow RED-GREEN pattern")

    # 3. Missing Dependencies
    print("\n3. MISSING DEPENDENCIES")
    print("-" * 80)
    issues = check_missing_dependencies(task_info)
    if issues:
        print("ISSUES FOUND:")
        for issue in issues:
            print(f"  - {issue}")
    else:
        print("Status: PASS - All dependencies correctly specified")

    # 4. Topological Sort
    print("\n4. EXECUTION ORDER (Topological Sort)")
    print("-" * 80)
    topo_order = topological_sort(task_info, graph)
    for i, task_id in enumerate(topo_order, 1):
        info = task_info[task_id]
        deps_str = ', '.join(info['depends_on']) if info['depends_on'] else 'none'
        print(f"{i:2d}. [{task_id}] {info['type']:6s} | deps: {deps_str:20s} | {info['subject']}")

    # 5. Mermaid Graph
    print("\n5. DEPENDENCY GRAPH (Mermaid Format)")
    print("-" * 80)
    mermaid = generate_mermaid(task_info, graph)
    print(mermaid)

    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)

if __name__ == '__main__':
    main()
