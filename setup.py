from setuptools import setup, find_packages

setup(
    name='pyxatu',
    version='1.7',
    packages=find_packages(),
    include_package_data=True,
    package_data={
        'pyxatu': ['config.json'],
    },
    install_requires=[
        'requests',
        'pandas',
        'tqdm',
        'bs4',
        'termcolor',
        'fastparquet',
        'click',
        'tabulate'
    ],
    entry_points={
        'console_scripts': [
            'xatu=pyxatu.cli:cli',
        ],
    },
    author='Toni Wahrstätter',
    author_email='toni@ethereum.org',
    description='A Python interface for the Xatu API',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/nerolation/pyxatu',
    classifiers=[
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.7',
)
