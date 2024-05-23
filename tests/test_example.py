"""
Examples of how to use unit tests within PyTest

Note that tests can also be ran against code in a different .py module.
An example of this is in 'test_data_quality_check_utils.py'
"""

import pytest

def some_function(input):
    """Example of a function we want to test the behaviour of"""
    return (input + 1)



def test_some_function_expected_behaviour():
    result = some_function(5)
    expected_result = 6
    assert expected_result == result

def test_some_function_string_input():
    """In this case we expect a TypeError"""
    with pytest.raises(TypeError):
        some_function('string input')
    
def test_some_function_no_input():
    """We expect a TypeError"""
    with pytest.raises(TypeError):
        some_function(None)

def test_some_function_negative_number():
    result = some_function(-5)
    expected_result = -4
    assert result == expected_result

