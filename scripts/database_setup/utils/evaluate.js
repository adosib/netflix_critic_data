// evaluate.js
const { JSDOM } = require('jsdom');
const fs = require('fs');

// Read the HTML file
const html = fs.readFileSync(process.argv[2], 'utf-8');

const dom = new JSDOM(html, {
    runScripts: "dangerously", // Allow running inline scripts (like in a browser)
    // resources: "usable" // Load external resources like scripts and stylesheets
  });

dom.window.addEventListener("load", () => {
    // After the page is loaded, you can access the 'netflix.reactContext'
    const reactContext = dom.window.netflix.reactContext;
  
    // Now access the data you want to stringify
    const sectionData = reactContext.models.nmTitleUI.data.sectionData;
    console.log(JSON.stringify(sectionData));
  });