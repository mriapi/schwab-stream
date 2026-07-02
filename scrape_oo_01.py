
# from playwright.sync_api import sync_playwright

# def run():
#     with sync_playwright() as p:
#         browser = p.chromium.launch(headless=False)
#         page = browser.new_page()

#         # Go directly to the shared backtest page (you are already logged in)
#         page.goto("https://optionomega.com/share/CRDemC1XPBFbZFnmtGG7")

#         # Wait for the button to appear
#         page.wait_for_selector("text='+ New Backtest'")

#         # Click the button
#         page.click("text='+ New Backtest'")

#         # Pause so you can see the result
#         page.wait_for_timeout(3000)

#         browser.close()

# if __name__ == "__main__":
#     run()





# from playwright.sync_api import sync_playwright

# with sync_playwright() as p:
#     browser = p.chromium.launch_persistent_context(
#         user_data_dir=r"C:/Users/mri17/AppData/Local/Google/Chrome/User Data",
#         headless=False,
#         channel="chrome"   # This tells Playwright to use real Chrome
#     )

#     page = browser.new_page()
#     page.goto("https://optionomega.com/share/CRDemC1XPBFbZFnmtGG7")
#     page.click("text='+ New Backtest'")    




# from playwright.sync_api import sync_playwright

# with sync_playwright() as p:
#     browser = p.chromium.launch_persistent_context(
#         user_data_dir=r"C:/Users/mri17/AppData/Local/Google/Chrome/User Data",
#         headless=False,
#         channel="chrome"
#     )

#     # Use the first (already open) page in the persistent context
#     page = browser.pages[0]

#     page.goto("https://optionomega.com/share/CRDemC1XPBFbZFnmtGG7")
#     page.click("text='+ New Backtest'")



# from playwright.sync_api import sync_playwright

# with sync_playwright() as p:
#     browser = p.chromium.launch_persistent_context(
#         user_data_dir=r"C:/Users/mri17/AppData/Local/Google/Chrome/User Data",
#         headless=False,
#         channel="chrome"
#     )

#     # Wait for Chrome to finish opening its initial tab
#     page = browser.wait_for_event("page")

#     page.goto("https://optionomega.com/share/CRDemC1XPBFbZFnmtGG7")
#     page.click("text='+ New Backtest'")








# from playwright.sync_api import sync_playwright

# with sync_playwright() as p:
#     browser = p.chromium.launch_persistent_context(
#         user_data_dir=r"C:/Users/mri17/AppData/Local/Google/Chrome/User Data",
#         headless=False,
#         channel="chrome"
#     )

#     # Wait for Chrome's startup tab
#     page = browser.wait_for_event("page")

#     page.goto("https://optionomega.com/share/CRDemC1XPBFbZFnmtGG7")
#     page.click("text='+ New Backtest'")    








# from playwright.sync_api import sync_playwright

# with sync_playwright() as p:
#     browser = p.chromium.launch_persistent_context(
#         user_data_dir=r"C:/Users/mri17/AppData/Local/Google/Chrome/User Data Playwright",
#         headless=False,
#         channel="chrome"
#     )

#     page = browser.wait_for_event("page")
#     page.goto("https://optionomega.com/share/CRDemC1XPBFbZFnmtGG7")
#     page.click("text='+ New Backtest'")    






# from playwright.sync_api import sync_playwright

# with sync_playwright() as p:
#     browser = p.chromium.launch_persistent_context(
#         user_data_dir=r"C:/Users/mri17/AppData/Local/Google/Chrome/User Data Playwright",
#         headless=False,
#         channel="chrome"
#     )

#     # Allow Chrome to finish its startup sequence
#     browser.wait_for_timeout(1000)

#     # Use the most recently opened tab
#     page = browser.pages[-1]

#     page.goto("https://optionomega.com/share/CRDemC1XPBFbZFnmtGG7")
#     page.click("text='+ New Backtest'")



from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch_persistent_context(
        user_data_dir=r"C:/Users/mri17/AppData/Local/Google/Chrome/User Data Playwright",
        headless=False,
        channel="chrome"
    )

    # Wait a moment for Chrome to finish opening its initial tab
    # (Chrome opens before Playwright fully attaches)
    while len(browser.pages) == 0:
        pass  # wait until at least one page exists

    page = browser.pages[-1]   # grab the visible tab
    page.wait_for_timeout(500) # small delay to stabilize

    page.goto("https://optionomega.com/share/CRDemC1XPBFbZFnmtGG7")
    page.click("text='+ New Backtest'")    