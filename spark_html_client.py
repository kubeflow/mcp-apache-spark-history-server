import asyncio
import os
import uuid
from urllib.parse import urljoin

from playwright.async_api import async_playwright

from config import ServerConfig

# HTML client for Spark History Server web interface


class SparkHtmlClient:
    def __init__(self, server_config: ServerConfig):
        self.config = server_config
        self.base_url = self.config.url.rstrip("/") + "/history/"
        self.auth = None
        self.browser = None

        # Set up authentication if provided
        if self.config.auth:
            if self.config.auth.username and self.config.auth.password:
                self.auth = (self.config.auth.username, self.config.auth.password)

    async def get_rendered_html(self, path):
        """
        Fetches and returns the fully rendered HTML content of a Spark UI page.

        This method uses Playwright to launch a headless browser, navigate to the specified path
        on the Spark UI, wait for the page to fully render (including any JavaScript execution),
        and then return the complete HTML content.

        Args:
            path: The path to navigate to, relative to the base URL

        Returns:
            str: The fully rendered HTML content of the page
        """
        async with async_playwright() as p:
            # Launch browser if not already launched
            if not self.browser:
                self.browser = await p.chromium.launch()
            page = await self.browser.new_page()
            await page.set_viewport_size({"width": 1280, "height": 800})
            url = urljoin(self.base_url, path)
            await page.goto(url)

            # Wait for network to be idle (no more than 2 connections for at least 500 ms)
            await page.wait_for_load_state("networkidle")

            await page.wait_for_timeout(3000)  # 3 seconds

            # Get the fully rendered HTML
            html_content = await page.content()

            return html_content

    async def get_screenshot(self, path, save_path=None):
        """
        Takes a screenshot of the page at the given path and saves it.

        Args:
            path: The path to navigate to for the screenshot
            save_path: The file path where the screenshot should be saved.
                       If None, saves to a random filename in /tmp.

        Returns:
            The full path to the saved screenshot
        """
        async with async_playwright() as p:
            if not self.browser:
                self.browser = await p.chromium.launch()
            path = path.lstrip("/")
            page = await self.browser.new_page()
            await page.set_viewport_size({"width": 2560, "height": 800})
            url = urljoin(self.base_url, path)
            await page.goto(url)

            # Wait for network to be idle
            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(3000)  # 3 seconds

            # Use provided save_path or generate a random filename
            import tempfile

            filename = (
                save_path
                if save_path
                else f"{tempfile.gettempdir()}/{uuid.uuid4()}.jpg"
            )

            # Ensure directory exists
            os.makedirs(os.path.dirname(os.path.abspath(filename)), exist_ok=True)
            await page.screenshot(
                path=filename, type="jpeg", quality=100, full_page=True
            )
            return filename


async def main():
    client = SparkHtmlClient(ServerConfig(url="http://localhost:18080"))

    # Example with custom save path
    await client.get_screenshot(
        "/spark-e975c1c221934381b99772c54ae4b8e6/jobs/", "spark_executors.jpg"
    )
    # html = await client.get_rendered_html("spark-e975c1c221934381b99772c54ae4b8e6/executors/")

    # Close the browser when done with all operations
    if client.browser:
        await client.browser.close()

    # Save the HTML to a file
    # with open("rendered_page.html", "w", encoding="utf-8") as f:
    #     f.write(html)
    #
    # print("HTML content saved to rendered_page.html")


if __name__ == "__main__":
    asyncio.run(main())
