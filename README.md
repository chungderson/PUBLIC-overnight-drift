# Overnight drift is rather slept on. This repo seeks to change that.

Attributions: [New York Fed Research, Boyarchenko et al.](https://www.newyorkfed.org/medialibrary/media/research/staff_reports/sr917.pdf)

I made thorough use of Windsurf, Cursor AI, Claude 3.7 Sonnet, ChatGPT, and all the other vibecoding tools. Details at bottom of the page.

# Setting it up
ps. you'll need to create an alpaca markets api key (it's free, and surprisingly simple to do...) and then create a config.json file that looks like:

{
"ALPACA_KEY": "get your own",
"ALPACA_SECRET": "haha u thought"
}

and pps. only the 30MinBarAttributes works... i was too lazy to segment it further to 5 and 15 min lmfao

# Visualizations
You'll get some results like:

![overnight drift](https://github.com/user-attachments/assets/ac8779de-6589-401a-b529-47ee46d13489)

or, over a longer timeframe:

![spy](https://github.com/user-attachments/assets/0af92407-0d09-455d-9c78-4a671e684c41)

or, if you want to volatility cluster:

![Figure_1](https://github.com/user-attachments/assets/538774db-941f-43c1-aae8-5a0d7acc9213)

These are only some of the things you can do with Alpaca and Pandas. 

It is important to note that I have almost zero coding experience, and I literally learned how to use VSC and Git by vibecoding. However, I want to take time to thank my AP CS A teacher Mrs. Susan King, found [here](https://www.linkedin.com/in/susan-king-00335bba/) for her incredible love for teaching and her inspiring outlook on programming. She's retired now, which is simultaneously amazing for her and also a huge loss to all future students.
