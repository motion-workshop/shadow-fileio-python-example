#
# Copyright (c) 2021, Motion Workshop
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice,
#    this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#
import os
import unittest

from main import main


class TestFileIOExample(unittest.TestCase):

    def test_main(self):
        filename_list = main([])

        self.assertIsInstance(filename_list, list)
        self.assertEqual(len(filename_list), 1)

        filename_list = main(['--format=avro', '--output=out.avro'])

        self.assertIsInstance(filename_list, list)
        self.assertEqual(len(filename_list), 1)

        filename_list = main(['--format=csv', '--quiet'])

        self.assertIsInstance(filename_list, list)
        self.assertEqual(len(filename_list), 1)

        # Trim off the YYYY-MM-DD/NNNN portion of the take path prefix. Use it
        # to test the other variant of find_newest_take that part of the path.
        tmp, filename = os.path.split(filename_list[0])
        tmp, number = os.path.split(tmp)
        tmp, date = os.path.split(tmp)
        tmp = os.path.join(date, number)

        # Try 1-4 positional filename arguments.
        for i in range(1, 5):
            filename_list = main([tmp] * i + ['--format=avro'])

            self.assertIsInstance(filename_list, list)
            self.assertEqual(len(filename_list), i)


if __name__ == '__main__':
    unittest.main()
