"""
Fix para o problema de event loop closed no Windows com Python 3.13
"""
import sys
if sys.platform == 'win32':
    import asyncio
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
