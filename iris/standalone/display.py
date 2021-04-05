from typing import Optional

from rich.console import Console, RenderGroup
from rich.highlighter import ReprHighlighter
from rich.panel import Panel
from rich.pretty import Pretty
from rich.table import Table
from rich.text import Text


def display_results(results: dict, console: Optional[Console] = None):
    if not console:
        console = Console(force_terminal=True, force_interactive=True)

    console.print()

    items_table = Table.grid(padding=(0, 1), expand=False)
    items_table.add_column(justify="right")
    items_table.add_row(
        "agent uuid =",
        Pretty(
            results["agent_uuid"],
            highlighter=ReprHighlighter(),
        ),
    )
    items_table.add_row(
        "database name =",
        Pretty(
            results["database_name"],
            highlighter=ReprHighlighter(),
        ),
    )
    items_table.add_row(
        "table name =",
        Pretty(
            results["table_name"],
            highlighter=ReprHighlighter(),
        ),
    )
    items_table.add_row(
        "n_rounds =",
        Pretty(
            results["n_rounds"],
            highlighter=ReprHighlighter(),
        ),
    )
    items_table.add_row("")
    items_table.add_row(
        "start time =",
        Pretty(
            results["start_time"].isoformat(),
            highlighter=ReprHighlighter(),
        ),
    )
    items_table.add_row(
        "end time =",
        Pretty(
            results["end_time"].isoformat(),
            highlighter=ReprHighlighter(),
        ),
    )

    console.print(
        Panel.fit(
            RenderGroup(
                "",
                Panel(
                    Text(results["measurement_uuid"], style="bold blue"),
                    title="measurement uuid",
                ),
                "",
                items_table,
            ),
            title="measurement results",
            border_style="scope.border",
            padding=(0, 1),
        )
    )
