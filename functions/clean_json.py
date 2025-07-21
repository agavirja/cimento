import math

def clean_json(obj):
    if obj is None:
        return None
    elif isinstance(obj, (int, str, bool)):
        return obj
    elif isinstance(obj, float):
        if math.isnan(obj):
            return None 
        elif math.isinf(obj):
            return None  
        else:
            return obj
    elif isinstance(obj, dict):
        return {key: clean_json(value) for key, value in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [clean_json(item) for item in obj]
    else:
        return str(obj)  # Convertir otros tipos a string