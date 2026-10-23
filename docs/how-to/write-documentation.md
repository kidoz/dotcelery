# Add or update a documentation page

[How-to guides](README.md) / Add documentation

Use this guide when documenting a feature or repairing an existing page. Keep
documentation beside the current implementation and describe the checked-out
behavior. The [roadmap](../../ROADMAP.md) is the source for known gaps and plans.

## Choose the reader's need

Pick one form before writing, following [Diátaxis](https://diataxis.fr/start-here/):

| Reader's need         | Location       | Include                                                   |
| --------------------- | -------------- | --------------------------------------------------------- |
| Learn by doing        | `tutorials/`   | Prerequisites, guided steps, observable results           |
| Complete a known task | `how-to/`      | A specific goal, required conditions, steps, verification |
| Look up facts         | `reference/`   | Contracts, names, defaults, constraints, source links     |
| Understand a design   | `explanation/` | Relationships, reasoning, tradeoffs, limitations          |

Give a page one main purpose. Link to another form when a lesson needs a long
design discussion or a procedure needs a large options table.

## Add the page and navigation

1. Create a descriptive Markdown filename under the chosen directory.
2. Add one title and a link back to the category index.
3. Add a link in that category's `README.md`.
4. Add it to the main [documentation index](../README.md) if it is a common entry point.
5. When moving existing content, repair relative links and leave a useful link at
   the old entry point. Preserve public README headings used by existing links.

Keep development policy in [Contributing](../../CONTRIBUTING.md), release history
in the [changelog](../../CHANGELOG.md), and feature status in the roadmap. Link to
those documents instead of making competing copies.

## Verify the content

- Check names, defaults, and paths against source and tests.
- Run complete tutorial code and check the stated output.
- Verify procedures against the relevant sample or test; report prerequisites
  you could not exercise.
- Check local links, fragments, code fences, and shell syntax before submitting.
- Use relative links within the repository and complete URLs for external sources.

Documentation changes require the relevant document and example checks; they do
not automatically require every runtime or container test.
