ALL_TABLES = """# Source Plugin: {{ plugin_name }}
## Tables
{%- for table in tables %}
{{ all_tables_entry(table) }}
{%- endfor %}"""

ALL_TABLES_ENTRY = """{{ indent_table_to_depth(table) }}- [{{ table.name }}]({{ table.name }}.md){% if table.is_incremental %} (Incremental){% endif %}
{%- for rel in table.relations %}
{{ all_tables_entry(rel) }}
{%- endfor %}"""

TABLE = """# Table: {{ table.name }}

This table shows data for {{ table.title }}.

{{ table.description }}
{% set length = table.primary_keys | length %}
{% if length == 0 %}
This table does not have a primary key.
{% elif length == 1 %}
The primary key for this table is **{{ table.primary_keys[0] }}**.
{% else %}
The composite primary key for this table is (
{%- for pk in table.primary_keys %}
    {{- ", " if loop.index > 1 else "" }}**{{ pk }}**
{%- endfor -%}
).
{% endif %}

{% if table.is_incremental %}
It supports incremental syncs{% set ik_length = table.incremental_keys | length %}
{%- if ik_length == 1 %} based on the **{{ table.incremental_keys[0] }}** column{% else %} based on the (
{%- for ik in table.incremental_keys %}
    {{- ", " if loop.index > 1 else "" }}**{{ ik }}**
{%- endfor -%}
) columns{% endif %}.
{%- endif %}

{% if table.relations or table.parent %}
## Relations
{% endif %}
{% if table.parent %}
This table depends on [{{ table.parent.name }}]({{ table.parent.name }}.md).
{% endif %}
{% if table.relations %}
The following tables depend on {{ table.name }}:
{% for rel in table.relations %}
  - [{{ rel.name }}]({{ rel.name }}.md)
{%- endfor %}
{% endif %}

## Columns
| Name          | Type          |
| ------------- | ------------- |
{%- for column in table.columns %}
|{{ column.name }}{% if column.primary_key %} (PK){% endif %}{% if column.incremental_key %} (Incremental Key){% endif %}|`{{ column.type }}`|
{%- endfor %}
"""
