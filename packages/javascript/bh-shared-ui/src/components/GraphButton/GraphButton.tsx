// Copyright 2023 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

import { Button, ButtonProps } from '@mui/material';
import makeStyles from '@mui/styles/makeStyles';
import { FC, ReactNode } from 'react';

const useStyles = makeStyles((theme) => ({
    button: {
        fontSize: '1rem',
        height: '1rem',
        lineHeight: '1rem',
        padding: theme.spacing(1.5),
        border: 'none',
        boxSizing: 'initial',
        borderRadius: theme.shape.borderRadius,
        backgroundColor: theme.palette.neutral.secondary,
        color: theme.palette.color.primary,
        textTransform: 'capitalize',
        minWidth: 'initial',
        '&:hover': {
            backgroundColor: theme.palette.neutral.tertiary,
            '@media (hover: none)': {
                backgroundColor: theme.palette.neutral.tertiary,
            },
        },
    },
}));

export interface GraphButtonProps extends ButtonProps {
    displayText: string | ReactNode;
}

const GraphButton: FC<GraphButtonProps> = (props) => {
    const { displayText } = props;
    const attributes = { ...props };
    delete attributes.displayText;
    const styles = useStyles();

    return (
        <Button {...attributes} disableRipple classes={{ root: styles.button }}>
            {displayText}
        </Button>
    );
};

export default GraphButton;
