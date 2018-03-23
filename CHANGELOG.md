# Versioning in a multi-package repo
You'll notice that this repo contains multiple package, each with its own versioning. To manage that, each package updates independently for minor and build changes, but major changes must be coordinated and linked across all packages.

The changelog is organized to reflect this. All packages maintain their own changelog with independent minor and build changes. However, major changes require coordination, which start here.

# 0.1 to 0.2

## 0.1.0 to 0.1.1
- Upgraded to GHC8, base 4.9
- Added docs describing future development 
- Replaced redundant `overJust`
