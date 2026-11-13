"""Loaded automatically by Gunicorn from the deployed application directory."""

# The lab keeps dashboard events in process memory. A single worker ensures all
# requests see the same buffer; threads let publishing and dashboard polls overlap.
workers = 1
threads = 4

# Import app.py in the worker so its Event Hubs thread is never started before fork.
preload_app = False
