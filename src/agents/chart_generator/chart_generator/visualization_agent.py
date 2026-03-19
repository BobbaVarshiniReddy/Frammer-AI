from data_profiler import profile_data
from llm_planner import get_plot_plan_llm
from utils import validate_plan
from code_generator import generate_code
from executor import execute_code


def visualization_agent(df, kpis, user_query, model):
    
    # Step 1: schema
    schema = profile_data(df)

    # Step 2: LLM plan
    raw_plan = get_plot_plan_llm(kpis, schema, user_query, model)

    # Step 3: validate
    plan = validate_plan(raw_plan, df)

    # Step 4: generate code
    code = generate_code(plan)

    # Step 5: execute
    execute_code(code, df)

    return {
        "plan": plan,
        "code": code
    }