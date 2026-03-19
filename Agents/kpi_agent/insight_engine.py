def generate_insight(name, value):
    if value is None:
        return "Not enough data"

    name = name.lower()

    if "rate" in name or "conversion" in name:
        if value < 0.01:
            return "Very low conversion → major inefficiency"
        elif value < 0.1:
            return "Low performance, needs improvement"
        else:
            return "Healthy conversion"

    if "growth" in name:
        if value < 0:
            return "Declining trend"
        else:
            return "Positive growth"

    return "Useful monitoring metric"