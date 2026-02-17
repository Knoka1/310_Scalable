#
# Unit tests for URL shortener service. To add more unit tests,
# add functions within the class below such that the function
# name starts with "test".
#
# To run:
#   python3 tests.py
#
# Initial template:
#   Prof. Joe Hummel
#   Northwestern University
# Alec do Couto

import shorten
import unittest
import uuid

  
############################################################
#
# Unit tests
#
class URLShortenerTests(unittest.TestCase):
    #
    # NOTE: a unit test must start with "test" in order to be
    # discovered by Python's unit testing framework.
    #
    # unit test #1:
    #
    def test_basic_api(self):
      print()
      print("** test_basic_api: basic test of API functions **")

      #
      # generate some unique URLs:
      #
      longurl = "https://" + str(uuid.uuid4()) + ".html"
      shorturl = "https://" + str(uuid.uuid4())

      #
      # make some calls:
      #

      # short url not yet present:
      count = shorten.get_stats(shorturl)
      self.assertEqual(count, -1)

      # map long to short:
      success = shorten.put_shorturl(longurl, shorturl)
      self.assertEqual(success, True)

      # stats should be 0:
      count = shorten.get_stats(shorturl)
      self.assertEqual(count, 0)

      # now lookup short url:
      url = shorten.get_url(shorturl)
      self.assertEqual(url, longurl)

      # stats should now be 1:
      count = shorten.get_stats(shorturl)
      self.assertEqual(count, 1)

      # legal to shorten the same long to short url:
      success = shorten.put_shorturl(longurl, shorturl)
      self.assertEqual(success, True)

      # empty the database:
      success = shorten.put_reset()
      self.assertEqual(success, True)

      # short url is now gone:
      count = shorten.get_stats(shorturl)
      self.assertEqual(count, -1)
      url = shorten.get_url(shorturl)
      self.assertEqual(url, "")
      
      #
      # end of test
      #
      print("test passed!")

      
############################################################
#
# main
#
if __name__ == '__main__':
  unittest.main()
