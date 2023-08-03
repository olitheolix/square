"""Dummy entry point script for the Square package.

Its primary purpose is to provide a single entrypoint script for PyInstaller.

You may invoke Square from this folder with `python -m square` or `python runme.py`.

"""
import asyncio

import square
import square.main

asyncio.run(square.main.main())
