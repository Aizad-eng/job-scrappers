"""
Template Engine - Handles variable substitution in actor configurations.

Supports:
- Simple variables: {{field_name}}
- Nested field access: {{parent.child.grandchild}}
- Array access: {{array[0]}}
- Array transformations: {{field_array}} converts text to array
"""

import re
from typing import Any, Dict, List, Union
import logging

logger = logging.getLogger(__name__)


def get_nested_value(data: Dict, path: str, default: Any = None) -> Any:
    """
    Get a value from nested dict/list using dot notation.
    
    Examples:
        get_nested_value({"a": {"b": 1}}, "a.b") -> 1
        get_nested_value({"items": [1,2,3]}, "items[0]") -> 1
        get_nested_value({"a": {"b": [{"c": 1}]}}, "a.b[0].c") -> 1
    """
    if not path or not data:
        return default
    
    try:
        current = data
        
        # Split by dots, but handle array notation
        parts = re.split(r'\.(?![^\[]*\])', path)
        
        for part in parts:
            if not part:
                continue
            
            # Check for array notation like "field[0]"
            array_match = re.match(r'(.+?)\[(\d+)\]$', part)
            
            if array_match:
                field_name = array_match.group(1)
                index = int(array_match.group(2))
                
                if isinstance(current, dict):
                    current = current.get(field_name, [])
                
                if isinstance(current, list) and len(current) > index:
                    current = current[index]
                else:
                    return default
            else:
                if isinstance(current, dict):
                    current = current.get(part, default)
                    if current is None:
                        return default
                else:
                    return default
        
        return current
    except Exception as e:
        logger.debug(f"Error getting nested value '{path}': {e}")
        return default


def render_template(template: Any, variables: Dict[str, Any]) -> Any:
    """
    Render a template by substituting variables.
    
    Handles:
    - String templates: "Hello {{name}}" -> "Hello John"
    - Dict templates: {"key": "{{value}}"} -> {"key": "actual_value"}
    - List templates: ["{{item1}}", "{{item2}}"] -> ["val1", "val2"]
    - Special suffixes: {{field_array}} converts newline/comma text to array
    """
    if template is None:
        return None
    
    if isinstance(template, str):
        return _render_string(template, variables)
    
    if isinstance(template, dict):
        return {k: render_template(v, variables) for k, v in template.items()}
    
    if isinstance(template, list):
        return [render_template(item, variables) for item in template]
    
    # For other types (int, bool, etc.), return as-is
    return template


def _render_string(template: str, variables: Dict[str, Any]) -> Any:
    """
    Render a string template.
    
    If the entire string is a single variable (e.g., "{{urls}}"),
    return the actual value (preserving type).
    
    If mixed with text, return string with substitutions.
    """
    if not isinstance(template, str):
        return template
    
    # Check if it's a pure variable reference (e.g., "{{urls}}")
    pure_var_match = re.match(r'^\{\{(\w+(?:_array)?)\}\}$', template.strip())
    
    if pure_var_match:
        var_name = pure_var_match.group(1)
        
        # Handle array suffix transformation
        if var_name.endswith('_array'):
            base_name = var_name[:-6]  # Remove '_array' suffix
            value = variables.get(base_name, '')
            return _to_array(value)
        
        return variables.get(var_name)
    
    # Mixed template - substitute all variables as strings
    def replace_var(match):
        var_name = match.group(1)
        
        # Handle array suffix
        if var_name.endswith('_array'):
            base_name = var_name[:-6]
            value = variables.get(base_name, '')
            arr = _to_array(value)
            return str(arr)
        
        value = variables.get(var_name, '')
        return str(value) if value is not None else ''
    
    result = re.sub(r'\{\{(\w+(?:_array)?)\}\}', replace_var, template)
    return result


def _to_array(value: Any) -> List[str]:
    """
    Convert a value to an array.
    
    - If already a list, return it
    - If string with newlines, split by newlines
    - If string with commas, split by commas
    - Otherwise, return single-element list
    """
    if value is None or value == '':
        return []
    
    if isinstance(value, list):
        return [str(v).strip() for v in value if v]
    
    if isinstance(value, str):
        # First try newlines
        if '\n' in value:
            return [line.strip() for line in value.split('\n') if line.strip()]
        # Then try commas
        if ',' in value:
            return [item.strip() for item in value.split(',') if item.strip()]
        # Single value
        return [value.strip()] if value.strip() else []
    
    return [str(value)]


def extract_fields(data: Dict, mapping: Dict[str, str]) -> Dict[str, Any]:
    """
    Extract fields from data using a mapping.
    
    mapping: {"output_field": "source.path.to.field"}
    
    Returns dict with output_field keys and extracted values.
    """
    result = {}
    
    for output_field, source_path in mapping.items():
        value = get_nested_value(data, source_path)
        result[output_field] = value
    
    return result


def build_clay_payload(extracted_data: Dict, clay_template: Dict, extra_fields: Dict = None) -> Dict:
    """
    Build Clay webhook payload from extracted data using template.
    
    clay_template: {"Clay Field": "{{extracted_field}}", ...}
    extra_fields: Additional fields to add (e.g., keyword, platform)
    """
    # Merge extracted data with extra fields
    variables = {**extracted_data}
    if extra_fields:
        variables.update(extra_fields)
    
    payload = {}
    
    for clay_field, template in clay_template.items():
        value = render_template(template, variables)
        
        # Clean up the value
        if isinstance(value, str):
            value = value.strip()
            # Remove empty strings
            if not value:
                value = None
        elif isinstance(value, list):
            # Join lists for Clay
            value = ", ".join(str(v) for v in value if v)
        
        payload[clay_field] = value
    
    # Add extra fields directly
    if extra_fields:
        for key, value in extra_fields.items():
            if key not in payload:
                payload[key] = value
    
    return payload


def clean_payload(payload: Dict) -> Dict:
    """Remove None values and clean up payload for API submission"""
    cleaned = {}
    
    for key, value in payload.items():
        # Skip None values
        if value is None:
            continue
        
        # Skip empty strings for optional fields
        if value == '' and key not in ['count', 'limit', 'max_results']:
            continue
        
        # Skip empty lists
        if isinstance(value, list) and len(value) == 0:
            continue
        
        cleaned[key] = value
    
    return cleaned
