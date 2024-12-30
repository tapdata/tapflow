from setuptools import setup, find_packages
with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='tapflow',
    version='0.2.52',
    packages=find_packages(),
    install_requires=required,
    include_package_data=True,
    package_data={
        '': ['*.md', '*.txt', '*.sh', "*.py", '*.project'],
    },
    entry_points={
        'console_scripts': [
            'tap=tapflow.cli.tap:main',
        ],
    },
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
)
