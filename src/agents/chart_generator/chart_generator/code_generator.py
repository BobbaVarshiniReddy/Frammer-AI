PLOT_TEMPLATES = {
    "line": """
fig, ax = plt.subplots()
df.groupby("{x}")["{y}"].{agg}().plot(ax=ax)
ax.set_title("{title}")
plt.show()
""",

    "bar": """
fig, ax = plt.subplots()
df.groupby("{x}")["{y}"].{agg}().plot(kind="bar", ax=ax)
ax.set_title("{title}")
plt.show()
""",

    "scatter": """
fig, ax = plt.subplots()
df.plot.scatter(x="{x}", y="{y}", ax=ax)
ax.set_title("{title}")
plt.show()
""",

    "histogram": """
fig, ax = plt.subplots()
df["{y}"].plot(kind="hist", ax=ax)
ax.set_title("{title}")
plt.show()
""",

    "box": """
fig, ax = plt.subplots()
df.boxplot(column="{y}", by="{x}", ax=ax)
plt.title("{title}")
plt.suptitle("")
plt.show()
""",

    "pie": """
fig, ax = plt.subplots()
df.groupby("{x}")["{y}"].{agg}().plot(kind="pie", ax=ax)
ax.set_title("{title}")
plt.show()
"""
}


def generate_code(plan):
    code_blocks = []

    for p in plan:
        template = PLOT_TEMPLATES.get(p["plot"])

        if not template:
            code_blocks.append(f"# Unsupported plot: {p['plot']}")
            continue

        code = template.format(
            x=p.get("x", ""),
            y=p.get("y", ""),
            agg=p.get("aggregation", "sum"),
            title=p.get("kpi", "Chart")
        )

        code_blocks.append(code)

    return "\n\n".join(code_blocks)