from setuptools import setup, find_packages
with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='tapflow',
    version='0.2.7',
    packages=find_packages(),
    install_requires=required,
    scripts=['tap'],
    include_package_data=True,
    package_data={
        '': ['*.md', '*.txt', '*.sh', "*.py"],
    },
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',  # Python 版本要求
)
