from setuptools import setup, find_packages
from setuptools.command.install import install
import os
from pathlib import Path
import shutil
import importlib.resources

class PostInstallCommand(install):
    """Post-installation for installation mode."""
    def run(self):
        install.run(self)
        self._copy_config_to_home()

    def _copy_config_to_home(self):
        # Path to home directory
        home = Path.home()
        user_config_path = home / '.pyxatu_config.json'

        # Dynamically load config.json from the pyxatu package
        try:
            with importlib.resources.path('pyxatu', 'config.json') as default_config_path:
                if not user_config_path.exists():
                    shutil.copy(default_config_path, user_config_path)
                    print(f"Default configuration copied to {user_config_path}. Please modify it with your actual credentials.")
                else:
                    print(f"User configuration already exists at {user_config_path}")
        except Exception as e:
            print(f"Error copying the configuration file: {e}")

setup(
    name='pyxatu',
    version='1.1',
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
        'fastparquet'
    ],
    entry_points={
        'console_scripts': [
            'xatu-query=pyxatu.cli:main',
        ],
    },
    cmdclass={
        'install': PostInstallCommand,
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