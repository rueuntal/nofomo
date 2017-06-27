# Test with foo first
def foo():
    print "success!" > open('../test.txt', 'a')


if __name__ == 'main':
    foo()
