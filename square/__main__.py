import sys

import square.main

if __name__ == '__main__':
    try:
        sys.exit(square.main.main())
    except KeyboardInterrupt:
        print("User abort")
        sys.exit(1)
