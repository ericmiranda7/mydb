#!/bin/bash
grep --exclude="todo_upd.sh" -rni "todo()" | sed 's|\(.*\):// todo():\s*\(.*\)|- [ ] \2 \1|' > todos.md
