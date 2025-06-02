// Custom JavaScript for Transmog documentation

document.addEventListener('DOMContentLoaded', function () {
    // Add copy buttons to all code blocks that don't already have them
    // (this is a fallback in case sphinx-copybutton doesn't work)
    if (typeof window.copyCellContent !== 'function') {
        document.querySelectorAll('div.highlight pre').forEach(function (block) {
            // Only add button if one doesn't exist already
            if (!block.parentNode.querySelector('.copybutton')) {
                var button = document.createElement('button');
                button.className = 'copybutton';
                button.textContent = 'Copy';

                button.addEventListener('click', function () {
                    var text = block.textContent;
                    navigator.clipboard.writeText(text).then(function () {
                        button.textContent = 'Copied!';
                        setTimeout(function () {
                            button.textContent = 'Copy';
                        }, 2000);
                    });
                });

                block.parentNode.insertBefore(button, block);
            }
        });
    }

    // Make external links open in a new tab
    document.querySelectorAll('a[href^="http"]').forEach(function (link) {
        if (!link.target) {
            link.target = '_blank';
            link.rel = 'noopener noreferrer';
        }
    });

    // Add version selector behavior if present
    var versionSelector = document.getElementById('version-selector');
    if (versionSelector) {
        versionSelector.addEventListener('change', function () {
            window.location.href = this.value;
        });
    }

    // Initialize any tabbed content
    document.querySelectorAll('.tabbed-set').forEach(function (tabContainer) {
        var tabs = tabContainer.querySelectorAll('.tabbed-set > input');
        tabs.forEach(function (tab) {
            tab.addEventListener('change', function () {
                localStorage.setItem('selectedTab', this.id);
            });
        });

        // Restore previously selected tab
        var selectedTab = localStorage.getItem('selectedTab');
        if (selectedTab) {
            var tab = document.getElementById(selectedTab);
            if (tab && tabContainer.contains(tab)) {
                tab.checked = true;
            }
        }
    });

    // Apply custom styling to admonitions
    customizeAdmonitions();

    // Add tutorial-to-example navigation widgets
    addTutorialExampleNavigation();
});

function customizeAdmonitions() {
    // Add custom styling to admonitions if needed
    const admonitions = document.querySelectorAll('.admonition');
    admonitions.forEach(admonition => {
        // Add any custom admonition styling here
    });
}

function addTutorialExampleNavigation() {
    // Tutorial to example mapping
    const tutorialExampleMap = {
        // Basic tutorials
        '/tutorials/basic/transform-nested-json': 'data_processing/basic/flattening_basics.py',
        '/tutorials/basic/flatten-and-normalize': 'data_processing/basic/flattening_basics.py',

        // Intermediate tutorials
        '/tutorials/intermediate/streaming-large-datasets': 'data_processing/advanced/streaming_processing.py',
        '/tutorials/intermediate/customizing-id-generation': 'data_transformation/advanced/deterministic_ids.py',

        // Advanced tutorials
        '/tutorials/advanced/error-recovery-strategies': 'data_processing/advanced/error_handling.py',
        '/tutorials/advanced/optimizing-memory-usage': 'data_processing/advanced/performance_optimization.py'
    };

    // Example to tutorial mapping
    const exampleTutorialMap = {
        'data_processing/basic/flattening_basics.py': [
            { path: '/tutorials/basic/transform-nested-json', name: 'Transform Nested JSON' },
            { path: '/tutorials/basic/flatten-and-normalize', name: 'Flatten and Normalize' }
        ],
        'data_processing/advanced/streaming_processing.py': [
            { path: '/tutorials/intermediate/streaming-large-datasets', name: 'Streaming Large Datasets' }
        ],
        'data_transformation/advanced/deterministic_ids.py': [
            { path: '/tutorials/intermediate/customizing-id-generation', name: 'Customizing ID Generation' }
        ],
        'data_processing/advanced/error_handling.py': [
            { path: '/tutorials/advanced/error-recovery-strategies', name: 'Error Recovery Strategies' }
        ],
        'data_processing/advanced/performance_optimization.py': [
            { path: '/tutorials/advanced/optimizing-memory-usage', name: 'Optimizing Memory Usage' }
        ]
    };

    // Check if we're on a tutorial page
    const currentPath = window.location.pathname;
    let tutorialPath = '';

    for (const path in tutorialExampleMap) {
        if (currentPath.includes(path)) {
            tutorialPath = path;
            break;
        }
    }

    if (tutorialPath) {
        // We're on a tutorial page, add navigation to example
        const examplePath = tutorialExampleMap[tutorialPath];
        const exampleUrl = `https://github.com/scottdraper8/transmog/blob/main/examples/${examplePath}`;

        // Create navigation widget
        createNavigationWidget('example', examplePath, exampleUrl);
    } else {
        // Check if we're on an example page in the documentation
        const examplePattern = /\/examples\/(.+?)\.html/;
        const match = currentPath.match(examplePattern);

        if (match) {
            const examplePath = match[1].replace(/\//g, '_') + '.py';
            const tutorials = exampleTutorialMap[examplePath];

            if (tutorials && tutorials.length) {
                // Create navigation widgets for each tutorial
                tutorials.forEach(tutorial => {
                    createNavigationWidget('tutorial', tutorial.name, tutorial.path);
                });
            }
        }
    }
}

function createNavigationWidget(type, name, url) {
    // Create widget element
    const widget = document.createElement('div');
    widget.className = 'tutorial-example-nav';

    const label = type === 'example' ? 'View Example Code:' : 'Related Tutorial:';

    widget.innerHTML = `
        <div class="tutorial-example-nav-header">
            ${label}
        </div>
        <div class="tutorial-example-nav-link">
            <a href="${url}" target="${type === 'example' ? '_blank' : '_self'}">${name}</a>
        </div>
    `;

    // Find insertion point (after title)
    const title = document.querySelector('h1');
    if (title && title.parentNode) {
        title.parentNode.insertBefore(widget, title.nextSibling);
    }
}
