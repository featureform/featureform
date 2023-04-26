import React from "react";
import "jest-canvas-mock";
import { cleanup, render } from "@testing-library/react";
import EntityPage from "../src/components/entitypage/EntityPage";
import ReduxWrapper from "../src/components/redux/wrapper/ReduxWrapper";
import { createSlice, configureStore } from "@reduxjs/toolkit";
import { ThemeProvider } from "@material-ui/core/styles";
import TEST_THEME from "../src/styles/theme";

jest.mock("../src/components/entitypage/EntityPageView", () => () => {
    return <div data-testid="entityPageViewId" />;
});

describe("Entity Page Tests", () => {
    const LOADING_DOTS_ID = "loadingDotsId";
    const NOT_FOUND = "notFoundId";
    const apiMock = { fetchEntity: jest.fn() };

    const defaultState = Object.freeze({
        entityPage: { loading: false, failed: false },
        selectedVariant: "",
    })

    const getTestBody = (initialState = {}) => {
        const slice = createSlice({
            name: "testSlice",
            initialState: initialState
        });
        const store = configureStore({
            reducer: slice.reducer,
        });

        return <>
            <ReduxWrapper store={store}>
                <ThemeProvider theme={TEST_THEME}>
                    <EntityPage api={apiMock} type="sources" entity="myEntity" />
                </ThemeProvider>
            </ReduxWrapper>
        </>;
    }

    beforeEach(() => {
        jest.resetAllMocks();
    })

    afterEach(() => {
        cleanup();
    })

    test("Issue-762: If the entity page fetch is loading, the loading component displays.", async () => {
        //given:
        const state = { ...defaultState, entityPage: { loading: true, failed: false } };
        const helper = render(getTestBody(state));

        //when: 
        const loadingDotsDiv = await helper.findByTestId(LOADING_DOTS_ID);

        //then: 
        expect(loadingDotsDiv).toBeDefined();
        expect(loadingDotsDiv.nodeName).toBe("DIV");
        expect(apiMock.fetchEntity).toHaveBeenCalledTimes(1);
    });

    test('Issue-762: If the fetch state fails, load the "404 not found" component', async () => {
        //given:
        const state = { ...defaultState, entityPage: { failed: true, loading: false } };
        const helper = render(getTestBody(state));

        //when: 
        const notFoundDiv = await helper.findByTestId(NOT_FOUND);
        const foundFoundElem = helper.getByText("404", { exact: false })

        //then: 
        expect(notFoundDiv).toBeDefined();
        expect(foundFoundElem.nodeName).toBe("H1");
        expect(apiMock.fetchEntity).toHaveBeenCalledTimes(1);
    });

    test('Issue-762: The fetch completed, but the returned object is empty, load the "404 not found" component', async () => {
        //given:
        const state = { ...defaultState, entityPage: { failed: false, loading: false } };
        const helper = render(getTestBody(state));

        //when: 
        const notFoundDiv = await helper.findByTestId(NOT_FOUND);
        const foundFoundElem = helper.getByText("404", { exact: false })

        //then: 
        expect(notFoundDiv).toBeDefined();
        expect(foundFoundElem.nodeName).toBe("H1");
        expect(apiMock.fetchEntity).toHaveBeenCalledTimes(1);
    });

    test("Issue-762: The fetch completed, and the returned object is populated, display the entity view component", async () => {
        //given:
        const foundObj = { name: "a name", type: "a type" }
        const state = { ...defaultState, entityPage: { failed: false, loading: false, resources: foundObj } };
        const helper = render(getTestBody(state));

        //when: 
        const foundPageMock = await helper.findByTestId("entityPageViewId");

        //then: 
        expect(foundPageMock).toBeDefined();
        expect(apiMock.fetchEntity).toHaveBeenCalledTimes(1);
    });

    test('Issue-769: If no resource data is found, display the "404 not found" component', async () => {
        //given: an empty resources response obj
        const state = { ...defaultState, entityPage: { failed: false, loading: false, resources: {} } };
        const helper = render(getTestBody(state));

         //when: 
         const notFoundDiv = await helper.findByTestId(NOT_FOUND);
         const foundFoundElem = helper.getByText("404", { exact: false })
 
         //then: 
         expect(notFoundDiv).toBeDefined();
         expect(foundFoundElem.nodeName).toBe("H1");
         expect(apiMock.fetchEntity).toHaveBeenCalledTimes(1);
    });
});
