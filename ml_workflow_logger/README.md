# System Design

## Overview

## Models

### Flow

Describes the Graph and the various steps for the object

```
{
    ???
}
```

### Flow Record

Transactional records where the actual logging is done

```
{
    ???
}
```

### Run

Each instance of the algorithm execution is captured by this

```
{
    ???
}
```

# How does this work

Step 1 - Create a new Flow describing how the program is supposed to work (or keep track of it)

Step 2 - Start a run for a given flow

Step 3 - Save run metrics

Step 4 - Save each step record

Step 5 - End Run + Save as CSV (for local)