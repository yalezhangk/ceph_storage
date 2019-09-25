import testtools


class TestCase(testtools.TestCase):
    """Test case base class for all unit tests."""

    def __init__(self, *args, **kwargs):
        super(TestCase, self).__init__(*args, **kwargs)
