import pandas as pd
import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

class ExtensibilityEngine:
    """
    Handles dynamic schema discovery, KPI formulation, and metadata management.
    Ensures the system can adapt to new columns and metrics without manual code changes.
    """
    def __init__(self, logic_path: Optional[str] = None):
        if logic_path is None:
            # Default to same directory as this file
            self.logic_path = Path(__file__).resolve().parent / "logic_rules.json"
        else:
            self.logic_path = Path(logic_path)
            
        self.logic = self._load_logic()

    def _load_logic(self) -> Dict[str, Any]:
        """Loads the logic registry; initializes if missing."""
        if not self.logic_path.exists():
            logger.warning(f"Logic path {self.logic_path} not found. Initializing new registry.")
            return {"existing_dimensions": {}, "kpi_registry": {}}
        try:
            with open(self.logic_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading logic rules: {e}")
            return {"existing_dimensions": {}, "kpi_registry": {}}

    def _save_logic(self):
        """Persists the updated logic registry to disk."""
        try:
            with open(self.logic_path, 'w') as f:
                json.dump(self.logic, f, indent=4)
        except Exception as e:
            logger.error(f"Error saving logic rules: {e}")

    def dynamic_discover_and_update(self, df: pd.DataFrame):
        """
        Scans DataFrame for unknown columns.
        Categorizes them into Dimensions (Text) or Metrics (Numeric).
        Updates logic_rules.json automatically.
        """
        existing_cols = []
        for dims in self.logic.get('existing_dimensions', {}).values():
            if isinstance(dims, list):
                existing_cols.extend(dims)
            else:
                existing_cols.append(dims)
        
        kpi_registry = self.logic.get('kpi_registry', {})
        new_found = False
        
        for col in df.columns:
            # Skip columns already in registry or base ID columns
            if col in existing_cols or col in kpi_registry or col.endswith('_id'):
                continue
                
            new_found = True
            # RULE A: If it's Text/Object -> It's a New Dimension
            if df[col].dtype == 'object':
                dim_name = f"dim_{col.lower().replace(' ', '_')}"
                if dim_name not in self.logic['existing_dimensions']:
                    self.logic['existing_dimensions'][dim_name] = [col]
                    logger.info(f"Detected New Dimension: {col}. Registered as {dim_name}")
            
            # RULE B: If it's Numeric -> It's a New Potential Metric
            elif pd.api.types.is_numeric_dtype(df[col]):
                if col not in kpi_registry:
                    kpi_registry[col] = {"formula": f"sum({col})", "type": "raw_count"}
                    logger.info(f"Detected New Metric: {col}. Registered as raw_count.")
                    
                    # Apply Proportionality Logic for automatic KPI formulation
                    self._apply_proportionality_logic(col, kpi_registry)

        if new_found:
            self._save_logic()
            logger.info("logic_rules.json updated with newly discovered metadata.")

    def _apply_proportionality_logic(self, new_metric: str, kpi_registry: Dict[str, Any]):
        """
        Automatically suggests derived metrics (rates) based on naming patterns.
        Example: If 'error_count' is found and 'processed_count' exists, create 'error_rate'.
        """
        base_metrics = ['total_count', 'processed_count', 'video_count', 'video_count_sec']
        
        # Check for error or failure patterns
        if any(word in new_metric.lower() for word in ['error', 'fail', 'miss', 'bad']):
            for base in base_metrics:
                if base in kpi_registry:
                    rate_name = new_metric.lower().replace('count', 'rate')
                    if rate_name == new_metric.lower():
                        rate_name = f"{new_metric.lower()}_rate"
                    
                    if rate_name not in kpi_registry:
                        kpi_registry[rate_name] = {
                            "formula": f"{new_metric} / {base}",
                            "type": "derived_rate",
                            "base_metric": base,
                            "numerator": new_metric
                        }
                        logger.info(f"Proportionality Logic: Auto-created KPI '{rate_name}' ({new_metric}/{base})")
                        break

    def get_dimensions(self) -> Dict[str, List[str]]:
        return self.logic.get('existing_dimensions', {})

    def get_metrics(self) -> Dict[str, Dict[str, Any]]:
        return self.logic.get('kpi_registry', {})
