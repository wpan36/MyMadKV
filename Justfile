# Project modules
mod utils 'justmod/utils.just'
mod p1 'justmod/proj1.just'
mod p2 'justmod/proj2.just'
mod p3 'justmod/proj3.just'

# List all available top-level recipes and modules
default:
    @just --list --unsorted

# Show a compact project directory tree
tree:
    tree -a -I '.git|build'