from setuptools import setup, find_packages
with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='tapcli',
    version='0.1',
    packages=find_packages(),
    install_requires=required,
    scripts=['tapcli'],
    include_package_data=True,
    package_data={
        '': ['*.md', '*.txt', '*.sh', "*.py", "*.ini"],
    },
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',  # Python 版本要求
)
