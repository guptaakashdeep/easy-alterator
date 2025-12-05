from collections import defaultdict
from collections import deque
from typing import Dict
from typing import List
from typing import Optional
from typing import Set


class ColumnPositionManager:
    def __init__(self):
        self.column_dependencies = {}  # column -> after_column
        self.reverse_dependencies = defaultdict(list)  # after_column -> [columns]
        self.all_columns = set()

    def extract_col_position(self, record_dicts: dict) -> Dict[str, Optional[str]]:
        """Extract column positioning information"""
        column_positions = {}

        for col_dict in record_dicts:
            column_name = col_dict["name"]
            after_column = col_dict["after"]

            column_positions[column_name] = after_column
            self.all_columns.add(column_name)

            if after_column:
                self.all_columns.add(after_column)

        return column_positions

    def build_dependency_graph(self, column_positions: Dict[str, Optional[str]]):
        """Build dependency graph from column positions.
        e.g. A -> B -> C -> D"""
        self.column_dependencies = column_positions.copy()
        self.reverse_dependencies = defaultdict(list)

        for column, after_column in column_positions.items():
            if after_column:
                self.reverse_dependencies[after_column].append(column)

    def get_affected_columns(
        self, changed_column: str, new_after: Optional[str]
    ) -> Set[str]:
        """Get all columns affected by moving one column"""
        affected = set()

        # Get old position
        # old_after = self.column_dependencies.get(changed_column)

        # Find all columns that were positioned after the moved column (cascade effect)
        def find_cascade_affected(col: str, visited: Set[str]):
            if col in visited:
                return
            visited.add(col)
            affected.add(col)

            # Find columns positioned after this column
            for dependent_col in self.reverse_dependencies[col]:
                find_cascade_affected(dependent_col, visited)

        # Start cascade from the changed column
        find_cascade_affected(changed_column, set())

        return affected

    def generate_position_changes(
        self, changes: Dict[str, Optional[str]]
    ) -> List[Dict[str, str]]:
        """Generate all position changes needed including cascades"""
        all_changes = []
        processed = set()

        for changed_column, new_after in changes.items():
            if changed_column in processed:
                continue

            # Get all affected columns
            affected_columns = self.get_affected_columns(changed_column, new_after)

            # Update dependencies
            # old_after = self.column_dependencies.get(changed_column)
            self.column_dependencies[changed_column] = new_after

            # Rebuild the chain for affected columns
            ordered_affected = self.topological_sort_affected(affected_columns)

            for col in ordered_affected:
                if col not in processed:
                    current_after = self.column_dependencies.get(col)
                    all_changes.append(
                        {
                            "name": col,
                            "after": current_after,
                            "reason": "cascade" if col != changed_column else "direct",
                        }
                    )
                    processed.add(col)

        return all_changes

    def topological_sort_affected(self, affected_columns: Set[str]) -> List[str]:
        """Sort affected columns in dependency order"""
        # Create subgraph of affected columns
        subgraph = defaultdict(list)
        in_degree = defaultdict(int)

        for col in affected_columns:
            in_degree[col] = 0

        for col in affected_columns:
            after_col = self.column_dependencies.get(col)
            if after_col and after_col in affected_columns:
                subgraph[after_col].append(col)
                in_degree[col] += 1

        # Topological sort
        queue = deque([col for col in affected_columns if in_degree[col] == 0])
        result = []

        while queue:
            col = queue.popleft()
            result.append(col)

            for dependent in subgraph[col]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        return result


# Usage example
# ddl_content = """
# `month_yr` string,
# `month` string,
# `batch_run_type` string,
# `orig_batch_type` string,
# `day_rk` date,
# `rawfile_rundate` date,
# `sovereign_pse_flag` string, -- after: cva_exemp_int_grp_comp_flag
# `sovereign_guarantee_flag` string, -- after: sovereign_pse_flag
# `sec_w_factor` decimal(12,7), -- after: sovereign_guarantee_flag
# `sec_k_sa` decimal(12,7), -- after: sec_w_factor
# """

# manager = ColumnPositionManager()
# positions = manager.parse_ddl(ddl_content)
# manager.build_dependency_graph(positions)

# # Simulate change: move sovereign_pse_flag after orig_batch_type
# # valid_after_Df in easy_alterator code
# changes = {'sovereign_pse_flag': 'orig_batch_type'}
# all_changes = manager.generate_position_changes(changes)

# for change in all_changes:
#     print(f"Column: {change['column']}, After: {change['after']}, Reason: {change['reason']}")
