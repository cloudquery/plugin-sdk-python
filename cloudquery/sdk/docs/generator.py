import json
import os
import pathlib
import re
from typing import List

import jinja2

from cloudquery.sdk.docs.markdown_templates import ALL_TABLES, ALL_TABLES_ENTRY, TABLE
from cloudquery.sdk.schema import Table


class JsonTable:
    def __init__(self):
        self.name = ""
        self.title = ""
        self.description = ""
        self.columns = []
        self.relations = []

    def to_dict(self):
        return {
            "name": self.name,
            "title": self.title,
            "description": self.description,
            "columns": [col.to_dict() for col in self.columns],
            "relations": [rel.to_dict() for rel in self.relations],
        }


class JsonColumn:
    def __init__(self):
        self.name = ""
        self.type = ""
        self.is_primary_key = None
        self.is_incremental_key = None

    def to_dict(self):
        return {
            "name": self.name,
            "type": self.type,
            "is_primary_key": self.is_primary_key,
            "is_incremental_key": self.is_incremental_key,
        }


class Generator:
    def __init__(self, plugin_name: str, tables: List[Table]):
        self._plugin_name = plugin_name
        self._tables = sorted(tables)

    def generate(self, directory: str, format: str):
        os.makedirs(directory, exist_ok=True)
        if format == "markdown":
            self._generate_markdown(directory)
        elif format == "json":
            self._generate_json(directory)

    def _generate_json(self, directory: str):
        json_tables = self._jsonify_tables(self._tables)
        buffer = bytes(json.dumps(json_tables, indent=2, ensure_ascii=False), "utf-8")
        output_path = pathlib.Path(directory) / "__tables.json"
        with output_path.open("wb") as f:
            f.write(buffer)
        return None

    def _jsonify_tables(self, tables):
        json_tables = []
        for table in tables:
            json_columns = []
            for col in table.columns:
                json_column = JsonColumn()
                json_column.name = col.name
                json_column.type = str(col.type)
                json_column.is_primary_key = col.primary_key
                json_column.is_incremental_key = col.incremental_key
                json_columns.append(json_column.__dict__)

            json_table = JsonTable()
            json_table.name = table.name
            json_table.title = table.title
            json_table.description = table.description
            json_table.columns = json_columns
            json_table.relations = self._jsonify_tables(table.relations)
            json_tables.append(json_table.__dict__)

        return json_tables

    def _generate_markdown(self, directory: str):
        env = jinja2.Environment()
        env.globals["indent_to_depth"] = self._indent_to_depth
        env.globals["all_tables_entry"] = self._all_tables_entry
        all_tables_template = env.from_string(ALL_TABLES)
        rendered_all_tables = all_tables_template.render(
            plugin_name=self._plugin_name, tables=self._tables
        )
        formatted_all_tables = self._format_markdown(rendered_all_tables)

        with open(os.path.join(directory, "README.md"), "w") as f:
            f.write(formatted_all_tables)

        for table in self._tables:
            self._render_table(directory, env, table)

    def _render_table(self, directory: str, env: jinja2.Environment, table: Table):
        table_template = env.from_string(TABLE)
        table_md = table_template.render(table=table)
        formatted_table_md = self._format_markdown(table_md)
        with open(os.path.join(directory, table.name + ".md"), "w") as f:
            f.write(formatted_table_md)
        for relation in table.relations:
            self._render_table(directory, env, relation)

    def _all_tables_entry(self, table: Table):
        env = jinja2.Environment()
        env.globals["indent_to_depth"] = self._indent_to_depth
        env.globals["all_tables_entry"] = self._all_tables_entry
        env.globals["indent_table_to_depth"] = self._indent_table_to_depth
        entry_template = env.from_string(ALL_TABLES_ENTRY)
        return entry_template.render(table=table)

    @staticmethod
    def _indent_table_to_depth(table: Table) -> str:
        s = ""
        t = table
        while t.parent is not None:
            s += "  "
            t = t.parent
        return s

    @staticmethod
    def _indent_to_depth(text: str, depth: int) -> str:
        indentation = depth * 4  # You can adjust the number of spaces as needed
        lines = text.split("\n")
        indented_lines = [(" " * indentation) + line for line in lines]
        return "\n".join(indented_lines)

    @staticmethod
    def _format_markdown(text: str) -> str:
        re_match_newlines = re.compile(r"\n{3,}")
        re_match_headers = re.compile(r"(#{1,6}.+)\n+")

        text = re_match_newlines.sub(r"\n\n", text)
        text = re_match_headers.sub(r"\1\n\n", text)
        return text
