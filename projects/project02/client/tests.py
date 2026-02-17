#
# Unit tests for photoapp API functions
#
# Initial tests:
#   Prof. Joe Hummel
#   Northwestern University
#

import photoapp
import unittest
import sys
import logging


############################################################
#
# Unit tests
#
class PhotoappTests(unittest.TestCase):
    #
    # NOTE: a unit test must start with "test" in order to be
    # discovered by Python's unit testing framework.
    #

  def test_01(self):
    print()
    print("** test_01: initialize **")

    success = photoapp.initialize('photoapp-client-config.ini')
    self.assertEqual(success, True)

    print("test passed!")

  def test_02(self):
    print()
    print("** test_02: get_ping **")

    (M, N) = photoapp.get_ping()

    self.assertEqual(M, 0)
    self.assertEqual(N, 3)

    print("test passed!")

  def test_03(self):
    print()
    print("** test_03: get_users **")

    correct = [(80001, 'p_sarkar', 'Pooja', 'Sarkar'), 
               (80002, 'e_ricci', 'Emanuele', 'Ricci'),
               (80003, 'l_chen', 'Li', 'Chen')]

    users = photoapp.get_users()

    self.assertEqual(users, correct)

    print("test passed!")


############################################################
#
# main
#

#
# eliminate traceback so we just get error message:
#
sys.tracebacklimit = 0

#
# capture logging output in file 'log.txt'
#
logging.basicConfig(
  filename='log.txt',
  level=logging.INFO,
  format='%(asctime)s - %(levelname)s - %(message)s',
  filemode='w'
)

if __name__ == '__main__':
  unittest.main()
