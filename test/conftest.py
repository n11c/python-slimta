# Monkey-patch before any test module imports ssl, socket, etc., as a real
# slimta application would. Patching later (e.g. from a test module) breaks
# libraries that already imported the unpatched ssl module.
import gevent.monkey

gevent.monkey.patch_all()
