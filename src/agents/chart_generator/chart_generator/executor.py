import matplotlib.pyplot as plt

def execute_code(code, df):
    local_vars = {
        "df": df,
        "plt": plt
    }

    try:
        exec(code, {}, local_vars)
    except Exception as e:
        print("Execution Error:", e)