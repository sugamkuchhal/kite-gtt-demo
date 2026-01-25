import argparse
import ast
import json
import os
import shlex
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
REQ_DIR = ROOT / "requirements"
IMPORT_MAP_PATH = REQ_DIR / "import_map.json"
ACTION_SCRIPTS_PATH = REQ_DIR / "action_scripts.json"
FULL_REQ = REQ_DIR / "full.txt"


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def parse_script_name(command: str) -> str:
    if not command:
        return ""
    parts = shlex.split(command)
    if not parts:
        return ""
    first = parts[0]
    if first in ("python", "python3"):
        if len(parts) < 2:
            return ""
        first = parts[1]
    name = os.path.basename(first)
    if name.endswith(".py"):
        name = name[:-3]
    return name


def stdlib_modules():
    names = set()
    names.update(sys.builtin_module_names)
    if hasattr(sys, "stdlib_module_names"):
        names.update(sys.stdlib_module_names)
    return names


STDLIB = stdlib_modules()


def is_stdlib(module_name: str) -> bool:
    root = module_name.split(".")[0]
    return root in STDLIB


def find_local_module(module_name: str) -> Path | None:
    root = module_name.split(".")[0]
    candidate = ROOT / f"{root}.py"
    if candidate.exists():
        return candidate
    candidate = ROOT / "extra" / f"{root}.py"
    if candidate.exists():
        return candidate
    return None


def collect_imports(py_file: Path, visited: set, imports: set, unknown: set):
    if py_file in visited:
        return
    visited.add(py_file)
    try:
        source = py_file.read_text(encoding="utf-8")
    except Exception:
        return
    try:
        tree = ast.parse(source, filename=str(py_file))
    except Exception:
        return

    const_strings, const_lists = collect_constant_assignments(tree)
    dynamic_modules, unresolved_dynamic = collect_dynamic_imports(tree, const_strings, const_lists)
    for module in dynamic_modules:
        handle_module(module, visited, imports, unknown)

    # If dynamic imports are unresolved, try heuristic package lists
    if unresolved_dynamic:
        heuristic = list(heuristic_dynamic_packages(const_lists))
        for module in heuristic:
            handle_module(module, visited, imports, unknown)
        # Only mark unresolved as unknown if heuristics found nothing
        if not heuristic:
            unknown.update(unresolved_dynamic)

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                module = alias.name
                handle_module(module, visited, imports, unknown)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                module = node.module
                handle_module(module, visited, imports, unknown)


def handle_module(module: str, visited: set, imports: set, unknown: set):
    if not module:
        return
    if is_stdlib(module):
        return
    local = find_local_module(module)
    if local:
        collect_imports(local, visited, imports, unknown)
        return
    imports.add(module)


def extract_string(node):
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Str):
        return node.s
    return None


def extract_string_list(node):
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        values = []
        for elt in node.elts:
            s = extract_string(elt)
            if s is None:
                return None
            values.append(s)
        return values
    return None


def collect_constant_assignments(tree):
    const_strings = {}
    const_lists = {}
    for node in ast.walk(tree):
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = []
            if isinstance(node, ast.Assign):
                targets = [t for t in node.targets if isinstance(t, ast.Name)]
            else:
                if isinstance(node.target, ast.Name):
                    targets = [node.target]
            if not targets:
                continue
            value = node.value
            s = extract_string(value)
            lst = extract_string_list(value)
            for t in targets:
                if s is not None:
                    const_strings[t.id] = s
                elif lst is not None:
                    const_lists[t.id] = lst
    return const_strings, const_lists


def is_dynamic_import_call(node):
    if not isinstance(node, ast.Call):
        return False
    fn = node.func
    if isinstance(fn, ast.Name) and fn.id == "__import__":
        return True
    if isinstance(fn, ast.Attribute):
        if isinstance(fn.value, ast.Name) and fn.value.id == "importlib":
            return fn.attr == "import_module"
    if isinstance(fn, ast.Name) and fn.id == "import_module":
        return True
    return False


def collect_dynamic_imports(tree, const_strings, const_lists):
    dynamic_modules = set()
    unresolved = set()
    for node in ast.walk(tree):
        if not is_dynamic_import_call(node):
            continue
        if not node.args:
            continue
        arg = node.args[0]
        s = extract_string(arg)
        if s:
            dynamic_modules.add(s)
            continue
        if isinstance(arg, ast.Name):
            name = arg.id
            if name in const_strings:
                dynamic_modules.add(const_strings[name])
                continue
            if name in const_lists:
                dynamic_modules.update(const_lists[name])
                continue
            unresolved.add(name)
            continue
        lst = extract_string_list(arg)
        if lst:
            dynamic_modules.update(lst)
            continue
        unresolved.add("<dynamic>")
    return dynamic_modules, unresolved


def heuristic_dynamic_packages(const_lists):
    packages = set()
    for name, values in const_lists.items():
        lowered = name.lower()
        if any(tok in lowered for tok in ("package", "packages", "module", "modules", "deps", "require")):
            packages.update(values)
    return packages


def map_imports_to_packages(imports: set, import_map: dict):
    packages = set()
    unknown = set()
    # sort keys for longest prefix match
    map_keys = sorted(import_map.keys(), key=len, reverse=True)
    for module in imports:
        matched = False
        for key in map_keys:
            if module == key or module.startswith(key + "."):
                packages.add(import_map[key])
                matched = True
                break
        if matched:
            continue
        root = module.split(".")[0]
        # default to root as package if it looks like a normal top-level module
        if root and root.isidentifier() and root not in STDLIB and not find_local_module(root):
            packages.add(root)
        else:
            unknown.add(module)
    return packages, unknown


def main():
    parser = argparse.ArgumentParser(description="Resolve dependency packages for an action/script.")
    parser.add_argument("--action", required=True, help="Action: script|refresh|midday|eod")
    parser.add_argument("--command", default="", help="Command for script mode (may include args)")
    parser.add_argument("--need-login", default="false", help="true if login is required")
    args = parser.parse_args()

    import_map = load_json(IMPORT_MAP_PATH)
    action_scripts = load_json(ACTION_SCRIPTS_PATH)

    action = args.action.strip().lower()
    need_login = str(args.need_login).lower() in ("1", "true", "yes")

    scripts = []
    if action == "script":
        script_name = parse_script_name(args.command)
        if script_name:
            scripts = [script_name]
    elif action == "refresh":
        scripts = ["auto_login"]
    else:
        scripts = action_scripts.get(action, [])

    if need_login and "auto_login" not in scripts:
        scripts.append("auto_login")

    # ensure_token always participates
    if "ensure_token" not in scripts:
        scripts.append("ensure_token")

    imports = set()
    visited = set()
    unresolved_dynamic = set()
    missing_scripts = []
    for script in scripts:
        if "/" in script:
            path = ROOT / f"{script}.py"
        else:
            path = ROOT / f"{script}.py"
            if not path.exists():
                alt = ROOT / "extra" / f"{script}.py"
                if alt.exists():
                    path = alt
        if not path.exists():
            missing_scripts.append(script)
            continue
        collect_imports(path, visited, imports, unresolved_dynamic)

    packages, unknown_imports = map_imports_to_packages(imports, import_map)
    if unresolved_dynamic:
        unknown_imports.update(unresolved_dynamic)

    use_full = False
    if unknown_imports or missing_scripts:
        use_full = True

    if use_full:
        print("packages=")
        print("use_full=true")
        print(f"req_file={FULL_REQ}")
    else:
        print(f"packages={' '.join(sorted(packages))}")
        print("use_full=false")
        if not packages:
            print("packages_empty=true")

    if unknown_imports:
        print(f"unknown_imports={','.join(sorted(unknown_imports))}")
    if missing_scripts:
        print(f"missing_scripts={','.join(sorted(missing_scripts))}")


if __name__ == "__main__":
    main()
