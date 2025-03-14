'''
udata search service
'''

import os

tag = os.environ.get('CIRCLE_TAG')
build_num = os.environ.get('CIRCLE_BUILD_NUM')

__version__ = '2.2.3.dev' + (str(build_num) if not tag and build_num else '')
