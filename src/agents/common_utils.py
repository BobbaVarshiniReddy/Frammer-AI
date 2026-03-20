import os
import glob
import ast

def get_existing_context(directory_path: str) -> str:
    """
    Scans a directory for .py files and extracts full function names and source code.
    """
    if not os.path.exists(directory_path):
        return ""
    
    context = []
    py_files = glob.glob(os.path.join(directory_path, "*.py"))
    
    for file_path in py_files:
        file_name = os.path.basename(file_path)
        try:
            with open(file_path, "r") as f:
                content = f.read()
                tree = ast.parse(content)
                
                context.append(f"### FILE: {file_name} ###")
                for node in ast.walk(tree):
                    if isinstance(node, ast.FunctionDef):
                        start_line = node.lineno - 1
                        end_line = node.end_lineno
                        source_lines = content.splitlines()[start_line:end_line]
                        source_code = "\n".join(source_lines)
                        
                        context.append(f"--- Function: {node.name} ---")
                        context.append(source_code)
                        context.append("")
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
            
    return "\n".join(context)
