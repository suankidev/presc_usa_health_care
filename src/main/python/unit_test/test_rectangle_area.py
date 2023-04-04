import unittest
import rectangle_area


class demoTest(unittest.TestCase):
    def test_rectangle(self):
        self.assertEqual(rectangle_area.rectangle_area(2, 3), 6)

    def test(self):
        self.assertEqual(6, 6)

    def test1(self):
        """is function able to handle """
        self.assertRaises(TypeError, rectangle_area.rectangle_area, 2, '2')


if __name__ == '__main__':
    unittest.main()
