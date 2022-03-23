from functions.validate import *


def test_verify_schema_version():
    """
    Function should only return True for schema URL with '2.0.0', '2.0.2' and 'latest'
    """
    schema_url_correct_1 = "http://abc/latest"
    schema_url_correct_2 = "http://abc/2.0.0"
    schema_url_correct_3 = "http://abc/2.0.2"
    schema_url_bad = "http://abc/not_a_real_schema"

    assert verify_schema_version(schema_url_correct_1)
    assert verify_schema_version(schema_url_correct_2)
    assert verify_schema_version(schema_url_correct_3)
    assert not verify_schema_version(schema_url_bad)
