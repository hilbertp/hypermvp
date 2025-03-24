import os
import subprocess
import sys

def run_dashboard():
    """Run the Streamlit dashboard from the correct directory."""
    # Get the absolute path to the app.py file
    dashboard_dir = os.path.dirname(os.path.abspath(__file__))
    app_path = os.path.join(dashboard_dir, "app.py")
    
    # Make sure the app.py file exists
    if not os.path.exists(app_path):
        print(f"Error: Dashboard file not found at {app_path}")
        sys.exit(1)
    
    # Run streamlit within the poetry environment
    try:
        # Print a message about how to stop the dashboard
        print("Starting HyperMVP Dashboard...")
        print("Press Ctrl+C to stop the dashboard")
        
        # Run the streamlit command
        subprocess.run(["streamlit", "run", app_path], check=True)
    except KeyboardInterrupt:
        print("\nDashboard stopped.")
    except Exception as e:
        print(f"Error running dashboard: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_dashboard()