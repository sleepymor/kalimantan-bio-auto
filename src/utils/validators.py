import re
from typing import Optional, List
from dataclasses import dataclass

@dataclass
class ValidationResult:
    is_valid: bool
    errors: List[str]
    warnings: List[str]

def validate_scientific_name(name: str) -> ValidationResult:
    errors = []
    warnings = []
    
    if not name or not name.strip():
        errors.append("Scientific name is empty")
        return ValidationResult(False, errors, warnings)
    
    name = name.strip()
    
    if len(name) < 3:
        errors.append("Scientific name too short")
    
    parts = name.split()
    if len(parts) < 2:
        warnings.append("Scientific name may be missing species epithet (only genus)")
    elif len(parts) > 4:
        warnings.append("Scientific name has many parts (possible subspecies/variety)")
    
    genus = parts[0]
    if not genus[0].isupper():
        errors.append("Genus should start with uppercase letter")
    
    if not re.match(r'^[A-Za-z]+$', genus):
        errors.append("Genus contains invalid characters")
    
    return ValidationResult(len(errors) == 0, errors, warnings)

def normalize_scientific_name(name: str) -> str:
    name = name.strip()
    name = re.sub(r'\s+', ' ', name)
    name = re.sub(r'\b(var\.|subsp\.|ssp\.|f\.|forma)\s+', r'\1 ', name, flags=re.IGNORECASE)
    return name

def validate_iucn_category(category: str) -> bool:
    valid = {"EX", "EW", "CR", "EN", "VU", "NT", "LC", "DD", "NE"}
    return category.upper() in valid

def validate_cites_appendix(appendix: str) -> bool:
    valid = {"I", "II", "III", "NOT_LISTED", "TIDAK TERCANTUM"}
    return appendix.upper() in valid

def validate_image_size_kb(size_kb: int, max_kb: int = 300) -> bool:
    return size_kb <= max_kb

def validate_webp_file(filepath: str) -> ValidationResult:
    errors = []
    warnings = []
    
    try:
        from PIL import Image
        with Image.open(filepath) as img:
            if img.format != "WEBP":
                errors.append(f"File is not WebP format: {img.format}")
            
            width, height = img.size
            if width > 1600 or height > 1600:
                warnings.append(f"Image dimensions ({width}x{height}) exceed 1600px")
            
            import os
            size_kb = os.path.getsize(filepath) / 1024
            if size_kb > 300:
                warnings.append(f"File size ({size_kb:.1f}KB) exceeds 300KB target")
                
    except Exception as e:
        errors.append(f"Failed to validate image: {e}")
    
    return ValidationResult(len(errors) == 0, errors, warnings)