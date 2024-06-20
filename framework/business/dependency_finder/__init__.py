import sqlglot
from typing import Set, List, Any


def get_table_names(query: str) -> Set[str]:
    if not query:
        return set()

    parsed_query = sqlglot.parse_one(query)

    # table name should be `schema_name.table_name`

    all_tables = set(
        [".".join(filter(None, [table.db, table.name])) for table in parsed_query.find_all(sqlglot.exp.Table)])
    # all_tables = set(filter([table.db, table.name]) for table in parsed_query.find_all(sqlglot.exp.Table))
    cte_names = set([cte.alias.lower() for cte in parsed_query.find_all(sqlglot.exp.CTE)])

    return all_tables - cte_names


def format_table_names(table_names: Set[str]) -> List[str]:
    output = []
    for table_name in sorted(table_names):
        db, name = table_name.split('.')
        if db == 'raw':
            output.append("/* {{ source('sap', '%s') }} */" % name)
        else:
            output.append("/* {{ ref('%s') }} */" % name)

    return output


def find_sources(table_names: Set[str]) -> Set[Any]:
    output = []
    for table_name in sorted(table_names):
        db, name = table_name.split('.')
        if db == 'raw':
            output.append(name)

    return set(output)


def update_models(models: List[str]) -> None:
    sources = set()

    for model in models:

        print(f"Updating {model}......", end='')

        # Read models, generate new dependencies after parsing sql
        query = open(model).read()
        table_names = get_table_names(query)
        dependencies = format_table_names(table_names)

        sources = sources.union(find_sources(table_names))

        output_file_lines = []

        for line in dependencies:
            output_file_lines.append(line)
            output_file_lines.append('\n')

        # Read the sql once more, and skip dependencies
        with open(model) as f2:
            query = f2.readlines()

            for line in query:
                if '{{ref' in line.replace(' ', ''): continue
                if '{{source' in line.replace(' ', ''): continue
                output_file_lines.append(line)

        # Update teh file in-place
        with open(model, 'w') as f:
            f.writelines(output_file_lines)

        print("Done")

    # Update sources.yml with all source names
    # TODO This is hacky. Assumes fixed whitespace! Use a proper YAML parser instead!
    with open('models/sources.yml') as f:
        sources_yml = f.readlines()
        updated_sources = []
        for line in sources_yml:
            if '      - name:' in line: continue
            updated_sources.append(line)

    for source in sorted(sources):
        updated_sources.append(f"      - name: {source}\n")

    with open('models/sources.yml', 'w') as f:
        f.writelines(updated_sources)


if __name__ == '__main__':
    import glob

    update_models(glob.glob('models/**/*.sql', recursive=True))
