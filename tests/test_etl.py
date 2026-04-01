"""Tests for the ETL pipeline.

Write at least 3 tests:
1. test_transform_filters_cancelled — cancelled orders excluded after transform
2. test_transform_filters_suspicious_quantity — quantities > 100 excluded
3. test_validate_catches_nulls — validate() raises ValueError on null customer_id
"""
import pandas as pd
import pytest
# استيراد الدوال من ملفك الرئيسي
from etl_pipeline import transform, validate

def test_transform_filters_cancelled():
    """Create test DataFrames with a cancelled order. Confirm it's excluded."""
    # إعداد بيانات وهمية: طلب واحد مكتمل وواحد ملغى
    data_dict = {
        'orders': pd.DataFrame({
            'order_id': [1, 2],
            'customer_id': [101, 101],
            'status': ['completed', 'cancelled']
        }),
        'order_items': pd.DataFrame({
            'order_id': [1, 2],
            'product_id': [50, 50],
            'quantity': [1, 1]
        }),
        'products': pd.DataFrame({
            'product_id': [50],
            'unit_price': [10.0]
        }),
        'customers': pd.DataFrame({
            'customer_id': [101],
            'customer_name': ['Hashem'],
            'city': ['Amman']
        })
    }
    
    result = transform(data_dict)
    
    # التأكد أن النتيجة تحتوي فقط على الطلب غير الملغى
    # بما أننا نجمع حسب العميل، يجب أن يكون total_orders هو 1 فقط
    assert result.iloc[0]['total_orders'] == 1
    assert len(result) == 1

def test_transform_filters_suspicious_quantity():
    """Create test DataFrames with quantity > 100. Confirm it's excluded."""
    data_dict = {
        'orders': pd.DataFrame({
            'order_id': [1, 2],
            'customer_id': [101, 101],
            'status': ['completed', 'completed']
        }),
        'order_items': pd.DataFrame({
            'order_id': [1, 2],
            'product_id': [50, 50],
            'quantity': [5, 150]  # الكمية 150 يجب أن تُحذف
        }),
        'products': pd.DataFrame({
            'product_id': [50],
            'unit_price': [10.0]
        }),
        'customers': pd.DataFrame({
            'customer_id': [101],
            'customer_name': ['Hashem'],
            'city': ['Amman']
        })
    }
    
    result = transform(data_dict)
    
    # التأكد أن الطلب المشبوه تم استبعاده وبقي الطلب العادي فقط
    assert result.iloc[0]['total_orders'] == 1

def test_validate_catches_nulls():
    """Create a DataFrame with null customer_id. Confirm validate() raises ValueError."""
    # إنشاء DataFrame يحتوي على قيمة Null في معرف العميل
    df_invalid = pd.DataFrame({
        'customer_id': [None],
        'customer_name': ['Test User'],
        'city': ['Amman'],
        'total_orders': [1],
        'total_revenue': [100.0]
    })
    
    # التأكد أن دالة validate ترفع الخطأ المطلوب عند وجود Null
    with pytest.raises(ValueError):
        validate(df_invalid)