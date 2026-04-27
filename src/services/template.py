"""Email template rendering service."""

import re
from typing import Any


def render_email(
    subject_template: str,
    body_template: str,
    **context: Any,
) -> tuple[str, str]:
    """
    Render email subject and body with context variables.
    
    Supports simple {{variable}} syntax.
    """
    subject = render_template(subject_template, context)
    body = render_template(body_template, context)
    return subject, body


def render_template(template: str, context: dict[str, Any]) -> str:
    """
    Render a single template string with context.
    
    Variables: {{variable_name}}
    Fallback: {{variable_name|default_value}}
    """
    def replace_var(match: re.Match) -> str:
        var_expr = match.group(1).strip()
        
        # Check for default value
        if "|" in var_expr:
            var_name, default = var_expr.split("|", 1)
            var_name = var_name.strip()
            default = default.strip()
        else:
            var_name = var_expr
            default = ""
        
        # Look up value
        value = context.get(var_name)
        if value is None:
            return default
        
        return str(value)
    
    # Replace {{var}} patterns
    pattern = r"\{\{([^}]+)\}\}"
    return re.sub(pattern, replace_var, template)


def validate_template(template: str) -> tuple[bool, str | None]:
    """
    Validate template syntax.
    
    Returns (is_valid, error_message).
    """
    # Check for unclosed braces
    open_count = template.count("{{")
    close_count = template.count("}}")
    
    if open_count != close_count:
        return False, f"Mismatched braces: {open_count} open, {close_count} close"
    
    # Check for empty variables
    if "{{" in template and "}}" in template:
        pattern = r"\{\{([^}]*)\}\}"
        matches = re.findall(pattern, template)
        for match in matches:
            if not match.strip():
                return False, "Empty variable name"
    
    return True, None
