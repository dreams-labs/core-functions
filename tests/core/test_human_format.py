import src.dreams_core.core

def test_human_format_zero():
    """ Test the human_format function with 0 as input. """
    assert src.dreams_core.core.human_format(0) == '0'
    
def test_human_format_decimal():
    """ Test the human_format function with a decimal number as input. """
    assert src.dreams_core.core.human_format(0.00037) == '0.00037'