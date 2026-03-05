from django import template

register = template.Library()

@register.filter(name='to_percentage')
def to_percentage(value, decimals=2):
    try:
        percentage = float(value) * 100
        format_string = f"{{:.{decimals}f}}"
        return format_string.format(percentage) + '%'
    except (ValueError, TypeError):
        return value