// evaluate.js
const { JSDOM } = require('jsdom');
const fs = require('fs');

let html;
// Read the HTML file
try {
  // Try to read the HTML file from the first argument
  html = fs.readFileSync(process.argv[2], 'utf-8');
} catch (error) {
  // If no file argument is provided, read from stdin
  const stdinBuffer = fs.readFileSync(0, 'utf-8');
  html = stdinBuffer.toString();
}

const dom = new JSDOM(html, {
  runScripts: "dangerously", // Allow running inline scripts (like in a browser)
  // resources: "usable" // Load external resources like scripts and stylesheets
});

dom.window.addEventListener("load", () => {
  try {
    // Access the 'netflix.reactContext' and extract the desired data
    const reactContext = dom.window.netflix.reactContext;
    const sectionData = reactContext.models.nmTitleUI.data.sectionData;
    console.log(JSON.stringify(sectionData));
  } catch (error) {
    console.error("Error extracting Netflix context:", error.message);
    process.exit(1);
  }
});