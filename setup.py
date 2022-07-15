from setuptools import setup, find_packages

setup(
    name='Firebase',
    version='5.0.1',
    url='https://github.com/09ghostrider/AsyncPyreBase-wrapper.git',
    description='A simple async python wrapper for the Firebase API',
    author='09ghostrider',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3.10',
    ],
    keywords='Firebase',
    packages=find_packages(exclude=['tests']),
    install_requires=[
        'oauth2client>=3.0.0',
        'aiohttp>=3.7.4',
        'pycryptodome>=3.4.3'
    ]
)