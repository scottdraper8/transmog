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

    // Smooth scroll to anchors
    document.querySelectorAll('a[href^="#"]').forEach(anchor => {
        anchor.addEventListener('click', function (e) {
            const targetId = this.getAttribute('href').substring(1);
            const targetElement = document.getElementById(targetId);

            if (targetElement) {
                e.preventDefault();
                targetElement.scrollIntoView({
                    behavior: 'smooth',
                    block: 'start'
                });

                // Update URL without triggering page reload
                history.pushState(null, null, this.getAttribute('href'));
            }
        });
    });

    // Add "back to top" button for long pages
    if (document.body.scrollHeight > window.innerHeight * 2) {
        addBackToTopButton();
    }
});

function addBackToTopButton() {
    const button = document.createElement('button');
    button.className = 'back-to-top';
    button.innerHTML = '↑';
    button.title = 'Back to top';
    button.style.cssText = `
        position: fixed;
        bottom: 2rem;
        right: 2rem;
        width: 3rem;
        height: 3rem;
        border-radius: 50%;
        background-color: var(--color-brand-primary);
        color: white;
        border: none;
        cursor: pointer;
        opacity: 0;
        transition: opacity 0.3s;
        z-index: 1000;
        font-size: 1.5rem;
        display: none;
    `;

    document.body.appendChild(button);

    // Show button when scrolled down
    window.addEventListener('scroll', function() {
        if (window.pageYOffset > 300) {
            button.style.display = 'block';
            setTimeout(() => button.style.opacity = '0.7', 10);
        } else {
            button.style.opacity = '0';
            setTimeout(() => button.style.display = 'none', 300);
        }
    });

    button.addEventListener('mouseenter', () => button.style.opacity = '1');
    button.addEventListener('mouseleave', () => button.style.opacity = '0.7');

    button.addEventListener('click', function() {
        window.scrollTo({
            top: 0,
            behavior: 'smooth'
        });
    });
}
